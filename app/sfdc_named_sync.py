"""
Salesforce *Named Query* delta syncs - Digital Enquiries and Gallery Footfall.

Same connected app / credentials as the Case sync (sfdc_case_sync), same
SQL Server database. Each feed is:

    GET {BASE}/services/data/{ver}/named/query/{QueryName}?lastsync=<UTC>

Every cycle: token -> GET with lastsync = (persisted watermark - overlap)
-> follow nextRecordsUrl while done=false -> upsert every row into a
schema-less table (one NVARCHAR(MAX) column per field, added on demand) ->
persist the new watermark. The watermark is the START time of the last
fully successful cycle, so a missed cycle / downtime self-heals on the
next run instead of leaving a hole.

Config (env, VG_ prefix; credentials are shared with the Case sync):
    VG_SFDC_CLIENT_ID / VG_SFDC_CLIENT_SECRET / VG_SFDC_BASE   (existing)
    VG_SFDC_NQ_API_VERSION      default v65.0
    VG_SFDC_NQ_INTERVAL_MIN     default 30
    VG_SFDC_NQ_OVERLAP_MIN      re-read window behind the watermark, default 5
    VG_SFDC_NQ_BACKFILL_START   first-run lastsync, default 2026-04-01T00:00:00Z

  Digital Enquiries  (enabled by default)
    VG_SFDC_DIGITAL_QUERY       default DigitalEnquiries
    VG_SFDC_DIGITAL_TABLE       default SFDC_DIGITAL_ENQUIRIES
  Gallery Footfall   (enabled once the named-query name is known)
    VG_SFDC_FOOTFALL_QUERY      e.g. GalleryFootfall   -- empty = feed idle
    VG_SFDC_FOOTFALL_TABLE      default SFDC_GALLERY_FOOTFALL
  Optional per feed:  VG_SFDC_<DIGITAL|FOOTFALL>_PARAM   (default lastsync)
                      VG_SFDC_<DIGITAL|FOOTFALL>_ID_FIELD (default Id)

Routes (per feed key = digital | footfall):
    /sfdc/<key>/health   status, row count, watermark, column list
    /sfdc/<key>/data     rows as JSON  (?limit=N&offset=M, newest first)
    /sfdc/<key>/sync     run one cycle now (?reset=1 = full re-backfill)
Standalone:  python sfdc_named_sync.py digital --once
"""

import os
import sys
import time
import hashlib
import json
import re
import threading
from datetime import datetime, timedelta, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sfdc_case_sync as base          # shared creds, SQL connection, helpers


def _env(name, default=""):
    return os.environ.get("VG_" + name, default)


API_VER = _env("SFDC_NQ_API_VERSION", "v65.0")
INTERVAL_MIN = int(_env("SFDC_NQ_INTERVAL_MIN", "30"))
OVERLAP_MIN = int(_env("SFDC_NQ_OVERLAP_MIN", "5"))
BACKFILL_START = _env("SFDC_NQ_BACKFILL_START", "2026-04-01T00:00:00Z")


def _log(msg):
    print(f"[sfdc_named_sync] {datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


class Feed:
    def __init__(self, key, default_query, default_table):
        self.key = key
        U = key.upper()
        self.query = _env(f"SFDC_{U}_QUERY", default_query)
        self.table = _env(f"SFDC_{U}_TABLE", default_table)
        self.state_table = self.table + "_SYNC_STATE"
        self.param = _env(f"SFDC_{U}_PARAM", "lastsync")
        self.id_field = _env(f"SFDC_{U}_ID_FIELD", "Id")
        self.lock = threading.Lock()
        self.status = {"feed": key, "query": self.query or None,
                       "enabled": bool(self.query),
                       "configured": bool(base.CLIENT_ID and base.CLIENT_SECRET),
                       "last_run": None, "last_ok": None, "last_error": None,
                       "last_fetched": 0, "last_upserted": 0,
                       "watermark": None, "runs": 0}


FEEDS = {
    "digital": Feed("digital", "DigitalEnquiries", "SFDC_DIGITAL_ENQUIRIES"),
    "footfall": Feed("footfall", "", "SFDC_GALLERY_FOOTFALL"),
}


# ---------------------------------------------------------------- SQL --

def _ensure_tables(cur, f):
    cur.execute(f"""
        IF OBJECT_ID('dbo.{f.table}','U') IS NULL
        CREATE TABLE dbo.{f.table} (
            SfdcId NVARCHAR(64) NOT NULL PRIMARY KEY,
            SyncedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )""")
    cur.execute(f"""
        IF OBJECT_ID('dbo.{f.state_table}','U') IS NULL
        CREATE TABLE dbo.{f.state_table} (
            Id INT NOT NULL PRIMARY KEY DEFAULT 1 CHECK (Id = 1),
            Watermark NVARCHAR(40) NULL,
            UpdatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )""")


def _columns(cur, f):
    cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?", f.table)
    return {r[0] for r in cur.fetchall()}


def _get_wm(cur, f):
    cur.execute(f"SELECT Watermark FROM dbo.{f.state_table} WHERE Id=1")
    r = cur.fetchone()
    return r[0] if r and r[0] else None


def _set_wm(cur, f, wm):
    cur.execute(f"""
        MERGE dbo.{f.state_table} AS t USING (SELECT 1 AS Id) AS s ON t.Id = s.Id
        WHEN MATCHED THEN UPDATE SET Watermark=?, UpdatedAt=SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (Id, Watermark) VALUES (1, ?);""", wm, wm)


def _upsert(cur, f, flats):
    if not flats:
        return 0
    have = _columns(cur, f)
    cols_all = sorted({c for r in flats for c in r} - {"_sid"})
    for c in cols_all:
        if c not in have and c != "SfdcId":
            cur.execute(f"ALTER TABLE dbo.{f.table} ADD [{c}] NVARCHAR(MAX) NULL")
            have.add(c)
    n = 0
    for r in flats:
        sid = r["_sid"]
        cols = [c for c in cols_all if c in r and c != "SfdcId"]
        vals = [r[c] for c in cols]
        if cols:
            sets = ", ".join(f"[{c}]=?" for c in cols)
            cur.execute(f"UPDATE dbo.{f.table} SET {sets}, SyncedAt=SYSUTCDATETIME() "
                        f"WHERE SfdcId=?", *vals, sid)
            hit = cur.rowcount
        else:
            hit = 0
        if hit == 0:
            collist = "".join(f", [{c}]" for c in cols)
            ph = "".join(", ?" for _ in cols)
            cur.execute(f"INSERT INTO dbo.{f.table} (SfdcId{collist}) VALUES (?{ph})",
                        sid, *vals)
        n += 1
    return n


# ----------------------------------------------------------- Salesforce --

_tok = {"v": None, "t": 0.0}


def _token():
    # one token serves every feed; refreshed every 20 min (client-credentials
    # tokens live far longer, this just keeps long backfills safe)
    if _tok["v"] and time.time() - _tok["t"] < 1200:
        return _tok["v"]
    _tok["v"], _tok["t"] = base._token(), time.time()
    return _tok["v"]


def _records_of(j):
    """Named-query bodies are normally {totalSize, done, records, nextRecordsUrl};
    tolerate a bare list or a single wrapper key holding the list."""
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        if isinstance(j.get("records"), list):
            return j["records"]
        for v in j.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return []


def _pages(f, lastsync_iso, token):
    """Yield one response page at a time; nextRecordsUrl is followed exactly
    as returned (per the SFDC doc)."""
    url = f"{base.BASE}/services/data/{API_VER}/named/query/{f.query}"
    params = {f.param: lastsync_iso}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    while True:
        r = requests.get(url, params=params, headers=headers, timeout=120)
        r.raise_for_status()
        j = r.json()
        yield _records_of(j)
        nxt = j.get("nextRecordsUrl") if isinstance(j, dict) else None
        if isinstance(j, dict) and j.get("done", True) is False and nxt:
            url, params = base.BASE + nxt, None
        else:
            return


def _row(f, rec):
    flat = base._flatten(rec)
    sid = flat.get(base._sanitize(f.id_field)) or flat.get("Id") or flat.get("id")
    if not sid:     # no id in the payload: stable hash of the row itself
        sid = "h_" + hashlib.sha1(json.dumps(rec, sort_keys=True, default=str).encode()).hexdigest()[:40]
    flat["_sid"] = str(sid)[:64]
    flat.pop("Id", None)
    return flat


# ---------------------------------------------------------------- sync --

def sync_once(key, reset=False):
    f = FEEDS[key]
    with f.lock:
        st = f.status
        st["runs"] += 1
        st["last_run"] = datetime.now(timezone.utc).isoformat()
        if not f.query:
            st["last_error"] = f"idle: set VG_SFDC_{key.upper()}_QUERY to the named-query name"
            return dict(st)
        if not (base.CLIENT_ID and base.CLIENT_SECRET):
            st["last_error"] = "not configured: set VG_SFDC_CLIENT_ID / VG_SFDC_CLIENT_SECRET"
            return dict(st)
        started = datetime.now(timezone.utc)
        try:
            conn = base._connect()
            cur = conn.cursor()
            _ensure_tables(cur, f)
            if reset:
                _set_wm(cur, f, None)
            wm = _get_wm(cur, f)
            if wm:
                cut = datetime.fromisoformat(wm.replace("Z", "+00:00")) - timedelta(minutes=OVERLAP_MIN)
                cutoff = cut.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                cutoff = BACKFILL_START
                _log(f"{key}: first run - backfilling from {cutoff}")
            token = _token()
            fetched = upserted = 0
            for recs in _pages(f, cutoff, token):
                fetched += len(recs)
                upserted += _upsert(cur, f, [_row(f, r) for r in recs if isinstance(r, dict)])
            new_wm = started.strftime("%Y-%m-%dT%H:%M:%SZ")
            _set_wm(cur, f, new_wm)           # only after every page succeeded
            conn.close()
            st.update({"last_ok": datetime.now(timezone.utc).isoformat(), "last_error": None,
                       "last_fetched": fetched, "last_upserted": upserted, "watermark": new_wm})
            _log(f"{key}: lastsync {cutoff} -> {fetched} rows, upserted {upserted}, watermark {new_wm}")
        except Exception as e:                 # noqa: BLE001 - keep the loop alive
            st["last_error"] = str(e)
            _log(f"{key}: ERROR {e}")
        return dict(st)


# ------------------------------------------------- Digital dataset (dashboard shape)

_STAGE_RANK = ["booked", "inventory", "in progress", "site visit", "new", "closed lost"]   # most advanced first
_dig_cache = {"key": None, "payload": None}


def _opp_stage(raw):
    """Opportunities__r_records (text of a list of dicts) -> most advanced stage, or ''."""
    import ast
    if not raw or raw in ("None", "[]"):
        return "", None
    try:
        recs = ast.literal_eval(raw)
    except Exception:                      # noqa: BLE001
        try:
            recs = json.loads(raw)
        except Exception:                  # noqa: BLE001
            return "", None
    stages, key_used = [], None
    for r in recs if isinstance(recs, list) else []:
        if not isinstance(r, dict):
            continue
        for k, v in r.items():
            if k != "attributes" and "stage" in k.lower() and v:
                stages.append(str(v)); key_used = key_used or k
                break
    if not stages:
        return "", None
    def rank(x):
        try: return _STAGE_RANK.index(x.strip().lower())
        except ValueError: return len(_STAGE_RANK)
    return sorted(stages, key=rank)[0], key_used


_NUM_DATE = re.compile(r"^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2}|\d{4})(?:[ ,T]+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([AaPp][Mm])?)?$")
_NAMED_FORMATS = ["%d-%b-%Y %H:%M:%S", "%d-%b-%Y", "%d %b %Y", "%b %d, %Y", "%d-%b-%y"]


def _date_order(values):
    """Decide month-first (MDY) or day-first (DMY) ONCE for the whole CreatedDate
    column, never per value: a value whose first number is >12 proves DMY, one
    whose second number is >12 proves MDY. With no proof, Salesforce's en_US
    FORMAT() order (MDY) is used. VG_SFDC_DIGITAL_DATEORDER=MDY|DMY pins it.
    Per-value guessing read 10/12/2026 (12 Oct) as 10 Dec - future months."""
    pinned = _env("VG_SFDC_DIGITAL_DATEORDER").strip().upper()
    if pinned in ("MDY", "DMY"):
        return pinned, "env"
    dmy = mdy = 0
    for v in values:
        m = _NUM_DATE.match(str(v or "").strip())
        if not m:
            continue
        a, b = int(m.group(1)), int(m.group(2))
        if a > 12 >= b:
            dmy += 1
        elif b > 12 >= a:
            mdy += 1
    if dmy > mdy:
        return "DMY", f"evidence dmy={dmy} mdy={mdy}"
    return "MDY", f"evidence dmy={dmy} mdy={mdy}" + ("" if mdy else " (default)")


def _created_day(cd, epoch, order="MDY"):
    """Salesforce CreatedDate (UTC ISO, or a FORMAT()-ed local string) -> day number since 2022-01-01 (IST), or -1."""
    if cd is None:
        return -1
    t = str(cd).strip()
    if not t or t.lower() == "none":
        return -1
    iso = t.replace("T", " ")
    try:                                    # ISO with offset / Z / millis -> UTC -> IST
        dt = datetime.strptime(iso[:19], "%Y-%m-%d %H:%M:%S")
        if iso[19:].strip().startswith(("Z", "+0000", ".000+0000", ".000Z")) or len(iso) > 19:
            dt += timedelta(hours=5, minutes=30)
        return (dt - epoch).days
    except ValueError:
        pass
    try:
        return (datetime.strptime(iso[:10], "%Y-%m-%d") - epoch).days
    except ValueError:
        pass
    m = _NUM_DATE.match(t)                  # FORMAT()-ed values are already in local time
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        mo, d = (a, b) if order == "MDY" else (b, a)
        if y < 100:
            y += 2000
        try:
            return (datetime(y, mo, d) - epoch).days
        except ValueError:
            return -1
    for fmt in _NAMED_FORMATS:
        try:
            return (datetime.strptime(t, fmt) - epoch).days
        except ValueError:
            continue
    return -1


_ENQ_CANDIDATES = ["Enquiry_Number__c", "Enquiry_No__c", "Enquiry_Num__c", "EnquiryNumber__c",
                   "Enquiry_ID__c", "Enquiry_Id__c", "Enquiry_Code__c", "Enquiry_Name__c"]
_ENQ_CODE = re.compile(r"^[A-Za-z]{0,6}[-_/]?\d{3,}[A-Za-z0-9-_/]*$")


def _pick_enq_col(cur, f, have):
    """Column holding the enquiry number: env VG_SFDC_DIGITAL_ENQCOL pins it;
    else the first known custom field present; else Name, but only when its
    values look like codes (ENQ-00123 / 102345), never person names."""
    pinned = _env("VG_SFDC_DIGITAL_ENQCOL").strip()
    if pinned:
        return (pinned if pinned in have else None), "env"
    for c in _ENQ_CANDIDATES:
        if c in have:
            return c, "candidate"
    for c in sorted(have):
        lc = c.lower()
        if "enq" in lc and ("num" in lc or "no" in lc) and "__r" not in lc:
            return c, "pattern"
    if "Name" in have:
        cur.execute(f"SELECT TOP 50 [Name] FROM dbo.{f.table} WHERE [Name] IS NOT NULL")
        vals = [str(r[0]).strip() for r in cur.fetchall()]
        if vals and sum(1 for v in vals if _ENQ_CODE.match(v)) >= 0.9 * len(vals):
            return "Name", "name-codes"
    return None, "none"


def _digital_dataset():
    """dbo.SFDC_DIGITAL_ENQUIRIES -> the dict-coded dataset the Digital Leads
    screens consume (same shape as the bundled digitalEnquiries.json)."""
    f = FEEDS["digital"]
    conn = base._connect(); cur = conn.cursor()
    _ensure_tables(cur, f)
    cur.execute(f"SELECT COUNT(*), MAX(SyncedAt) FROM dbo.{f.table}")
    n, mx = cur.fetchone()
    key = (int(n or 0), str(mx))
    if _dig_cache["key"] == key and _dig_cache["payload"]:
        conn.close(); return _dig_cache["payload"]
    have = _columns(cur, f)
    want = ["Enquiry_Sub_Source__c", "Project__r_Name", "Status__c", "Agency_Source__c",
            "Owner_Name", "Opportunities__r_records", "CreatedDate"]
    enq_col, enq_how = _pick_enq_col(cur, f, have)
    enq_candidates = sorted(c for c in have if "enq" in c.lower() or c == "Name")
    if enq_col:
        want.append(enq_col)
    cols = [c for c in want if c in have]
    cur.execute(f"SELECT {', '.join('[' + c + ']' for c in cols)} FROM dbo.{f.table}")
    ix = {c: i for i, c in enumerate(cols)}
    rows = cur.fetchall()
    wm = _get_wm(cur, f)
    conn.close()

    lex = {k: {} for k in ("SUB", "PRJ", "STA", "AGN", "OWN", "STG")}
    def g(r, c):
        return r[ix[c]] if c in ix and r[ix[c]] is not None else None
    parsed = []
    stage_key = None
    for r in rows:
        stg, sk = _opp_stage(g(r, "Opportunities__r_records"))
        stage_key = stage_key or sk
        parsed.append((g(r, "Enquiry_Sub_Source__c") or "", g(r, "Project__r_Name") or "",
                       g(r, "Status__c") or "", (g(r, "Agency_Source__c") or "").strip() or "-",
                       g(r, "Owner_Name") or "", stg, g(r, "CreatedDate"),
                       str(g(r, enq_col) or "").strip() if enq_col else ""))
        for k, v in zip(("SUB", "PRJ", "STA", "AGN", "OWN", "STG"), parsed[-1][:6]):
            if v != "":
                lex[k][v] = 1
    L = {k: sorted(v, key=lambda x: x.lower()) for k, v in lex.items()}
    pos = {k: {x: i for i, x in enumerate(v)} for k, v in L.items()}
    epoch = datetime(2022, 1, 1)
    order, order_why = _date_order([x[6] for x in parsed])
    today = (datetime.utcnow() + timedelta(hours=5, minutes=30) - epoch).days
    R = []
    bad = []
    for sub, prj, sta, agn, own, stg, cd, enq in parsed:
        day = _created_day(cd, epoch, order)
        if day < 0 and len(bad) < 6 and str(cd) not in bad:
            bad.append(str(cd))
        R.append([pos["SUB"].get(sub, -1), pos["PRJ"].get(prj, -1), pos["STA"].get(sta, -1),
                  pos["AGN"].get(agn, -1), pos["OWN"].get(own, -1), pos["STG"].get(stg, -1), day, enq])
    stamp = (wm or "")[:16].replace("T", " ")
    payload = {**L, "epoch": "2022-01-01", "R": R,
               "meta": {"rows": len(R), "source": "Salesforce named query DigitalEnquiries (SFDC_DIGITAL_ENQUIRIES)",
                        "asOn": f"live · synced {stamp} UTC", "note": "Customer PII (names, emails, phones) excluded; enquiry number included",
                        "live": True, "ver": f"{key[0]}-{key[1]}", "stageKey": stage_key,
                        "withStage": sum(1 for x in R if x[5] >= 0),
                        "noDate": sum(1 for x in R if x[6] < 0), "badDateSamples": bad,
                        "dateOrder": order, "dateOrderWhy": order_why,
                        "futureDated": sum(1 for x in R if x[6] > today + 1),
                        "enqCol": enq_col, "enqHow": enq_how, "enqCandidates": enq_candidates,
                        "withEnq": sum(1 for x in R if x[7])}}
    _dig_cache.update({"key": key, "payload": payload})
    return payload


def _loop(key):
    f = FEEDS[key]
    _log(f"{key}: scheduler started - every {INTERVAL_MIN} min "
         f"({'query ' + f.query if f.query else 'IDLE until VG_SFDC_' + key.upper() + '_QUERY is set'})")
    while True:
        sync_once(key)
        time.sleep(INTERVAL_MIN * 60)


def start_background_threads():
    for k in FEEDS:
        threading.Thread(target=_loop, args=(k,), daemon=True).start()


# ---------------------------------------------------------------- Flask --

def register(app):
    from flask import jsonify, request as rq

    @app.after_request
    def _cors(resp):
        if rq.path.startswith("/sfdc/digital") or rq.path.startswith("/sfdc/footfall"):
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Allow-Headers"] = "*"
        return resp

    def _feed(key):
        f = FEEDS[key]

        @app.route(f"/sfdc/{key}/health", endpoint=f"sfdc_{key}_health")
        def health():
            out = dict(f.status)
            try:
                conn = base._connect(); cur = conn.cursor()
                _ensure_tables(cur, f)
                cur.execute(f"SELECT COUNT(*) FROM dbo.{f.table}")
                out["rows_in_table"] = cur.fetchone()[0]
                out["watermark"] = _get_wm(cur, f)
                out["columns"] = sorted(_columns(cur, f) - {"SfdcId", "SyncedAt"})
                conn.close()
            except Exception as e:             # noqa: BLE001
                out["db_error"] = str(e)
            return jsonify(out)

        @app.route(f"/sfdc/{key}/data", endpoint=f"sfdc_{key}_data")
        def data():
            try:
                lim = max(1, min(int(rq.args.get("limit", 50000)), 200000))
                off = max(0, int(rq.args.get("offset", 0)))
                conn = base._connect(); cur = conn.cursor()
                _ensure_tables(cur, f)
                cols = sorted(_columns(cur, f) - {"SfdcId"})
                sel = ", ".join(f"[{c}]" for c in ["SfdcId"] + cols)
                cur.execute(f"SELECT {sel} FROM dbo.{f.table} ORDER BY SyncedAt DESC "
                            f"OFFSET {off} ROWS FETCH NEXT {lim} ROWS ONLY")
                names = ["SfdcId"] + cols
                rows = [dict(zip(names, [None if v is None else str(v) for v in r])) for r in cur.fetchall()]
                cur.execute(f"SELECT COUNT(*) FROM dbo.{f.table}")
                total = cur.fetchone()[0]
                wm = _get_wm(cur, f)
                conn.close()
                return jsonify({"ok": True, "rows": rows, "total": total,
                                "meta": {"feed": key, "query": f.query, "watermark": wm, "live": True}})
            except Exception as e:             # noqa: BLE001
                return jsonify({"ok": False, "error": str(e)}), 500

        @app.route(f"/sfdc/{key}/sync", methods=["GET", "POST"], endpoint=f"sfdc_{key}_sync")
        def sync_now():
            return jsonify(sync_once(key, reset=rq.args.get("reset") == "1"))

    for k in FEEDS:
        _feed(k)

    @app.route("/sfdc/digital/dataset")
    def sfdc_digital_dataset():
        try:
            return jsonify({"ok": True, **_digital_dataset()})
        except Exception as e:             # noqa: BLE001
            return jsonify({"ok": False, "error": str(e)}), 500



if __name__ == "__main__":
    k = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] in FEEDS else "digital"
    if "--once" in sys.argv:
        print(sync_once(k))
    else:
        _loop(k)
