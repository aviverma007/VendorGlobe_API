"""
Salesforce Case delta sync (per the SFDC team's integration doc).

Every cycle:  OAuth client-credentials token -> SOQL query of Case rows
changed since the watermark (SystemModstamp, with a safety overlap) ->
follow nextRecordsUrl until done=true -> upsert into SQL Server keyed
on the SFDC Id. The watermark is persisted in the DB, so a missed cycle
self-heals on the next run (no 30-minute data hole after downtime).

Config (env, VG_ prefix) -- CREDENTIALS ARE ENV-ONLY, never in code:
    VG_SFDC_BASE            default https://smartworld.my.salesforce.com
    VG_SFDC_CLIENT_ID       connected-app client id      -- REQUIRED
    VG_SFDC_CLIENT_SECRET   connected-app client secret  -- REQUIRED
    VG_SFDC_API_VERSION     default v66.0
    VG_SFDC_DB              database, default db_config.DB_NAME
    VG_SFDC_TABLE           default SFDC_CASES
    VG_SFDC_INTERVAL_MIN    cycle interval, default 30
    VG_SFDC_OVERLAP_MIN     re-read window behind the watermark, default 5
    VG_SFDC_BACKFILL_START  first-run cutoff, ISO UTC
                            (default 2020-01-01T00:00:00Z = full history)

Standalone:  python sfdc_case_sync.py --once     (single cycle)
             python sfdc_case_sync.py            (scheduler loop)
"""

import os
import re
import sys
import time
import threading
from datetime import datetime, timedelta, timezone

import requests
import pyodbc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db_config as cfg


def _env(name, default):
    return os.environ.get("VG_" + name, default)


BASE = _env("SFDC_BASE", "https://smartworld.my.salesforce.com").rstrip("/")
CLIENT_ID = _env("SFDC_CLIENT_ID", "")
CLIENT_SECRET = _env("SFDC_CLIENT_SECRET", "")
API_VER = _env("SFDC_API_VERSION", "v66.0")
DB_NAME = _env("SFDC_DB", cfg.DB_NAME)
TABLE = _env("SFDC_TABLE", "SFDC_CASES")
STATE_TABLE = TABLE + "_SYNC_STATE"
INTERVAL_MIN = int(_env("SFDC_INTERVAL_MIN", "30"))
OVERLAP_MIN = int(_env("SFDC_OVERLAP_MIN", "5"))
BACKFILL_START = _env("SFDC_BACKFILL_START", "2020-01-01T00:00:00Z")

# SOQL from the SFDC integration doc, plus SystemModstamp for the
# watermark. FORMAT(...) kept exactly as specified by their team.
SOQL_FIELDS = (
    "Id,Account.Name,CaseNumber,Subject,Priority,Description,"
    "Service_Category__c,FORMAT(CreatedDate),FORMAT(ClosedDate),"
    "Owner.Name,Number_of_Reassigns__c,Last_Internal_Comment__c,"
    "Case_Latest_Comment__c,Origin,CaseType__c,Status,TAT_Status__c,"
    "Case_Source__c,Area__c,Sub_Area__c,Project__r.Name,Property__r.Name,"
    "Booking__r.Name,Booking__r.SAP_Salesorder_ID__c,IsClosed,"
    "CreatedBy.Name,Created_Time__c,Closed_Date_date_only__c,"
    "FORMAT(Closed_Date_Time_Only__c),FORMAT(Email_Received_At__c),"
    "FORMAT(First_Response_At__c),First_Email_Response_Time_Text__c,"
    "FORMAT(Latest_Email_Received_At__c),FORMAT(Latest_Response_At__c),"
    "Total_Emails_Sent__c,Total_Emails_Received__c,"
    "Response_Time_Category__c,Resolution_Time_Category__c,"
    "Account.HNI__c,Active_Legal_Case__c,Case_Applicability__c,"
    "Case_Applicability_Reason__c,Team_Leader_name__c,"
    "Latest_Task_Subject_for_Case__c,FORMAT(Last_Task_created_Date__c),"
    "Parent.CaseNumber,SystemModstamp"
)

_lock = threading.Lock()
_status = {"configured": bool(CLIENT_ID and CLIENT_SECRET), "last_run": None,
           "last_ok": None, "last_error": None, "last_fetched": 0,
           "last_upserted": 0, "watermark": None, "runs": 0}


def _log(msg):
    print(f"[sfdc_case_sync] {datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


# ---------------------------------------------------------------- SQL --

def _connect():
    return pyodbc.connect(
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
        f"DATABASE={DB_NAME};Trusted_Connection=yes;TrustServerCertificate=yes;",
        autocommit=True,
    )


def _sanitize(name):
    return re.sub(r"[^0-9A-Za-z_]", "_", str(name)).strip("_") or "col"


def _ensure_tables(cur):
    cur.execute(f"""
        IF OBJECT_ID('dbo.{TABLE}','U') IS NULL
        CREATE TABLE dbo.{TABLE} (
            SfdcId NVARCHAR(18) NOT NULL PRIMARY KEY,
            SyncedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )""")
    cur.execute(f"""
        IF OBJECT_ID('dbo.{STATE_TABLE}','U') IS NULL
        CREATE TABLE dbo.{STATE_TABLE} (
            Id INT NOT NULL PRIMARY KEY DEFAULT 1 CHECK (Id = 1),
            Watermark NVARCHAR(40) NULL,
            UpdatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )""")


def _existing_columns(cur):
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?", TABLE)
    return {r[0] for r in cur.fetchall()}


def _ensure_columns(cur, cols, have):
    for c in cols:
        if c not in have:
            cur.execute(f"ALTER TABLE dbo.{TABLE} ADD [{c}] NVARCHAR(MAX) NULL")
            have.add(c)


def _get_watermark(cur):
    cur.execute(f"SELECT Watermark FROM dbo.{STATE_TABLE} WHERE Id=1")
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _set_watermark(cur, wm):
    cur.execute(f"""
        MERGE dbo.{STATE_TABLE} AS t
        USING (SELECT 1 AS Id) AS s ON t.Id = s.Id
        WHEN MATCHED THEN UPDATE SET Watermark=?, UpdatedAt=SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (Id, Watermark) VALUES (1, ?);""", wm, wm)


# ----------------------------------------------------------- Salesforce --

def _token():
    r = requests.post(
        f"{BASE}/services/oauth2/token",
        data={"grant_type": "client_credentials",
              "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _flatten(rec, prefix=""):
    """Record -> flat {column: text}. Relations (Account, Owner...) become
    Parent_Field columns; the 'attributes' blobs are dropped."""
    out = {}
    for k, v in rec.items():
        if k == "attributes":
            continue
        col = _sanitize(prefix + k)
        if isinstance(v, dict):
            out.update(_flatten(v, prefix=f"{prefix}{k}_"))
        elif v is None:
            out[col] = None
        elif isinstance(v, bool):
            out[col] = "1" if v else "0"
        else:
            out[col] = str(v)
    return out


def _fetch_since(cutoff_iso, token):
    soql = (f"SELECT {SOQL_FIELDS} FROM Case "
            f"WHERE SystemModstamp >= {cutoff_iso} ORDER BY SystemModstamp DESC")
    url = f"{BASE}/services/data/{API_VER}/query/"
    params = {"q": soql}
    headers = {"Authorization": f"Bearer {token}"}
    records, pages = [], 0
    while True:
        r = requests.get(url, params=params, headers=headers, timeout=120)
        r.raise_for_status()
        j = r.json()
        records.extend(j.get("records", []))
        pages += 1
        if j.get("done", True) or not j.get("nextRecordsUrl"):
            break
        # use nextRecordsUrl exactly as provided (per the SFDC doc)
        url = BASE + j["nextRecordsUrl"]
        params = None
        if pages > 500:          # safety stop: 500 pages ~ 1M rows
            _log("WARN: pagination stopped at 500 pages")
            break
    return records, pages


# ---------------------------------------------------------------- sync --

def _upsert(cur, flats):
    if not flats:
        return 0
    have = _existing_columns(cur)
    all_cols = sorted({c for f in flats for c in f} - {"Id"})
    _ensure_columns(cur, all_cols, have)
    n = 0
    for f in flats:
        sid = f.get("Id")
        if not sid:
            continue
        cols = [c for c in all_cols if c in f]
        sets = ", ".join(f"[{c}]=?" for c in cols)
        vals = [f[c] for c in cols]
        cur.execute(f"UPDATE dbo.{TABLE} SET {sets}, SyncedAt=SYSUTCDATETIME() "
                    f"WHERE SfdcId=?", *vals, sid)
        if cur.rowcount == 0:
            collist = ", ".join(f"[{c}]" for c in cols)
            ph = ", ".join("?" for _ in cols)
            cur.execute(f"INSERT INTO dbo.{TABLE} (SfdcId, {collist}) "
                        f"VALUES (?, {ph})", sid, *vals)
        n += 1
    return n


def sync_once():
    with _lock:
        _status["runs"] += 1
        _status["last_run"] = datetime.now(timezone.utc).isoformat()
        if not (CLIENT_ID and CLIENT_SECRET):
            _status["last_error"] = "not configured: set VG_SFDC_CLIENT_ID / VG_SFDC_CLIENT_SECRET"
            return _status
        try:
            conn = _connect()
            cur = conn.cursor()
            _ensure_tables(cur)
            wm = _get_watermark(cur)
            if wm:
                cut = datetime.fromisoformat(wm.replace("Z", "+00:00")) - timedelta(minutes=OVERLAP_MIN)
                cutoff = cut.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                cutoff = BACKFILL_START
                _log(f"first run — backfilling from {cutoff}")
            token = _token()
            records, pages = _fetch_since(cutoff, token)
            flats = [_flatten(r) for r in records]
            n = _upsert(cur, flats)
            stamps = [f.get("SystemModstamp") for f in flats if f.get("SystemModstamp")]
            if stamps:
                _set_watermark(cur, max(stamps))
            new_wm = _get_watermark(cur)
            conn.close()
            _status.update({"last_ok": datetime.now(timezone.utc).isoformat(),
                            "last_error": None, "last_fetched": len(records),
                            "last_upserted": n, "watermark": new_wm})
            _log(f"cutoff {cutoff} -> {len(records)} records / {pages} page(s), upserted {n}, watermark {new_wm}")
        except Exception as e:           # noqa: BLE001 — keep the loop alive
            _status["last_error"] = str(e)
            _log(f"ERROR: {e}")
        return dict(_status)


def run_forever():
    _log(f"scheduler started — every {INTERVAL_MIN} min "
         f"({'configured' if _status['configured'] else 'NOT CONFIGURED: waiting for env'})")
    while True:
        sync_once()
        time.sleep(INTERVAL_MIN * 60)


def start_background_thread():
    t = threading.Thread(target=run_forever, daemon=True)
    t.start()
    return t


# ---------------------------------------------------------------- Flask --

def register(app):
    from flask import jsonify

    @app.route("/sfdc/cases/health")
    def sfdc_cases_health():
        out = dict(_status)
        try:
            conn = _connect()
            cur = conn.cursor()
            _ensure_tables(cur)
            cur.execute(f"SELECT COUNT(*) FROM dbo.{TABLE}")
            out["rows_in_table"] = cur.fetchone()[0]
            out["watermark"] = _get_watermark(cur)
            conn.close()
        except Exception as e:           # noqa: BLE001
            out["db_error"] = str(e)
        return jsonify(out)

    @app.route("/sfdc/cases/sync", methods=["POST", "GET"])
    def sfdc_cases_sync_now():
        return jsonify(sync_once())


if __name__ == "__main__":
    if "--once" in sys.argv:
        print(sync_once())
    else:
        run_forever()
