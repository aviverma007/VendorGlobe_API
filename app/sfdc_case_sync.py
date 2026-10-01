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
# Only cases created this financial year onwards (FY starts 1 Apr)
CREATED_FROM = _env("SFDC_CREATED_FROM", "2026-04-01T00:00:00Z")
BACKFILL_START = _env("SFDC_BACKFILL_START", "2026-04-01T00:00:00Z")
MAX_PAGES_PER_RUN = int(_env("SFDC_MAX_PAGES_PER_RUN", "300"))

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
           "last_upserted": 0, "watermark": None, "runs": 0, "caught_up": None}


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


def _set_watermark(cur, wm):  # wm=None clears it
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


def _pages_since(cutoff_iso, token):
    """Yield (records, done_flag) one Salesforce page at a time.
    ASC order so a capped run can resume from the watermark next cycle
    without leaving holes in older history."""
    soql = (f"SELECT {SOQL_FIELDS} FROM Case "
            f"WHERE SystemModstamp >= {cutoff_iso} "
            f"AND CreatedDate >= {CREATED_FROM} ORDER BY SystemModstamp ASC")
    url = f"{BASE}/services/data/{API_VER}/query/"
    params = {"q": soql}
    headers = {"Authorization": f"Bearer {token}"}
    while True:
        r = requests.get(url, params=params, headers=headers, timeout=120)
        r.raise_for_status()
        j = r.json()
        done = bool(j.get("done", True)) or not j.get("nextRecordsUrl")
        yield j.get("records", []), done
        if done:
            return
        # use nextRecordsUrl exactly as provided (per the SFDC doc)
        url = BASE + j["nextRecordsUrl"]
        params = None


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


def sync_once(reset=False):
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
            if reset:
                _set_watermark(cur, None)
                _log("watermark reset — full backfill restarts")
            wm = _get_watermark(cur)
            if wm:
                cut = datetime.fromisoformat(wm.replace("Z", "+00:00")) - timedelta(minutes=OVERLAP_MIN)
                cutoff = cut.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                cutoff = BACKFILL_START
                _log(f"first run — backfilling from {cutoff}")
            token = _token()
            fetched = upserted = pages = 0
            caught_up = True
            for records, done in _pages_since(cutoff, token):
                pages += 1
                fetched += len(records)
                flats = [_flatten(r) for r in records]
                upserted += _upsert(cur, flats)
                stamps = [f.get("SystemModstamp") for f in flats if f.get("SystemModstamp")]
                if stamps:
                    _set_watermark(cur, max(stamps))   # advance as we go — resumable
                if pages % 25 == 0:
                    _log(f"  ... {pages} pages, {fetched} records so far")
                if not done and pages >= MAX_PAGES_PER_RUN:
                    caught_up = False
                    _log(f"page cap {MAX_PAGES_PER_RUN} reached — next cycle resumes from the watermark")
                    break
            new_wm = _get_watermark(cur)
            conn.close()
            _status.update({"last_ok": datetime.now(timezone.utc).isoformat(),
                            "last_error": None, "last_fetched": fetched,
                            "last_upserted": upserted, "watermark": new_wm,
                            "caught_up": caught_up})
            _log(f"cutoff {cutoff} -> {fetched} records / {pages} page(s), upserted {upserted}, "
                 f"watermark {new_wm}, caught_up={caught_up}")
        except Exception as e:           # noqa: BLE001 — keep the loop alive
            _status["last_error"] = str(e)
            _log(f"ERROR: {e}")
        return dict(_status)


def run_forever():
    _log(f"scheduler started — every {INTERVAL_MIN} min "
         f"({'configured' if _status['configured'] else 'NOT CONFIGURED: waiting for env'})")
    while True:
        sync_once()
        # while the backfill is still behind, keep going after a breather
        time.sleep(30 if _status.get("caught_up") is False else INTERVAL_MIN * 60)


def start_background_thread():
    t = threading.Thread(target=run_forever, daemon=True)
    t.start()
    return t



# ------------------------------------------------- dashboard dataset --

_DAY0 = datetime(2022, 1, 1)
_data_cache = {"key": None, "payload": None}


def _case_day(v):
    """SFDC date/datetime text -> day offset from 2022-01-01 (−1 blank).
    Handles ISO, 'YYYY-MM-DD', and FORMAT() locale strings like
    '30/9/2026, 12:10 pm' or '9/30/2026, 12:10 PM'."""
    if not v:
        return -1
    t = str(v).strip()
    if not t:
        return -1
    head = t.split(",")[0].split("T")[0].strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return (datetime.strptime(head, fmt) - _DAY0).days
        except ValueError:
            continue
    return -1


def _num_or(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _build_case_dataset():
    """dbo.SFDC_CASES -> the dict-coded dataset the Case Management tab
    consumes (same shape as the bundled caseManagement.json)."""
    conn = _connect()
    cur = conn.cursor()
    _ensure_tables(cur)
    wm = _get_watermark(cur)
    today = datetime.now().strftime("%Y-%m-%d")
    key = (wm, today)
    if _data_cache["key"] == key and _data_cache["payload"]:
        conn.close()
        return _data_cache["payload"]

    want = ["SfdcId", "CaseNumber", "Account_Name", "Status", "CaseType__c",
            "Priority", "Origin", "TAT_Status__c", "Area__c", "Sub_Area__c",
            "Project__r_Name", "Owner_Name", "Case_Applicability__c",
            "CreatedDate", "ClosedDate", "Closed_Date_date_only__c",
            "Account_HNI__c", "Active_Legal_Case__c", "Number_of_Reassigns__c",
            "Team_Leader_name__c"]
    have = _existing_columns(cur)
    cols = [c for c in want if c in have]
    cur.execute(f"SELECT {', '.join('[' + c + ']' for c in cols)} FROM dbo.{TABLE}")
    ix = {c: i for i, c in enumerate(cols)}

    def g(row, col):
        i = ix.get(col)
        return row[i] if i is not None else None

    lists = {k: [] for k in ("STA", "TYP", "PRI", "ORG", "TAT", "AREA",
                             "SUBA", "PRJ", "OWN", "APP", "TL")}
    seen = {k: {} for k in lists}

    def enc(kind, val):
        v = (str(val).strip() if val not in (None, "") else "")
        if not v:
            return -1 if kind in ("TAT", "TL") else _enc_blank(kind)
        d = seen[kind]
        if v not in d:
            d[v] = len(lists[kind])
            lists[kind].append(v)
        return d[v]

    def _enc_blank(kind):
        return enc(kind, "—")

    today_day = (datetime.now() - _DAY0).days
    R = []
    for row in cur.fetchall():
        open_d = _case_day(g(row, "CreatedDate"))
        closed_d = _case_day(g(row, "Closed_Date_date_only__c"))
        if closed_d < 0:
            closed_d = _case_day(g(row, "ClosedDate"))
        age = (closed_d - open_d) if (closed_d >= 0 and open_d >= 0) else               (today_day - open_d) if open_d >= 0 else 0
        R.append([
            open_d, closed_d,
            enc("STA", g(row, "Status")), enc("TYP", g(row, "CaseType__c")),
            enc("PRI", g(row, "Priority")), enc("ORG", g(row, "Origin")),
            enc("TAT", g(row, "TAT_Status__c")), enc("AREA", g(row, "Area__c")),
            enc("SUBA", g(row, "Sub_Area__c")), enc("PRJ", g(row, "Project__r_Name")),
            enc("OWN", g(row, "Owner_Name")), enc("APP", g(row, "Case_Applicability__c")),
            max(age, 0),
            str(g(row, "Account_Name") or ""),
            str(g(row, "CaseNumber") or ""),
            1 if str(g(row, "Account_HNI__c")) == "1" else 0,
            1 if str(g(row, "Active_Legal_Case__c")) == "1" else 0,
            _num_or(g(row, "Number_of_Reassigns__c")),
            enc("TL", g(row, "Team_Leader_name__c")),
        ])
    conn.close()

    stamp = (wm or "")[:16].replace("T", " ")
    payload = {**lists, "R": R,
               "meta": {"rows": len(R), "asOn": f"live · synced {stamp} UTC",
                        "source": "Salesforce sync (SFDC_CASES)",
                        "watermark": wm, "live": True}}
    _data_cache.update({"key": key, "payload": payload})
    return payload

# ---------------------------------------------------------------- Flask --

def register(app):
    from flask import jsonify, request as _rq

    @app.after_request
    def _sfdc_cors(resp):
        if _rq.path.startswith("/sfdc"):
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Allow-Headers"] = "*"
        return resp

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

    @app.route("/sfdc/cases/data")
    def sfdc_cases_data():
        try:
            return jsonify({"ok": True, **_build_case_dataset()})
        except Exception as e:           # noqa: BLE001
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/sfdc/cases/sync", methods=["POST", "GET"])
    def sfdc_cases_sync_now():
        from flask import request as _r
        return jsonify(sync_once(reset=_r.args.get("reset") == "1"))


if __name__ == "__main__":
    if "--once" in sys.argv:
        print(sync_once())
    else:
        run_forever()
