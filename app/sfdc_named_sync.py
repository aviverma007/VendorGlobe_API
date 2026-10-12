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


if __name__ == "__main__":
    k = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] in FEEDS else "digital"
    if "--once" in sys.argv:
        print(sync_once(k))
    else:
        _loop(k)
