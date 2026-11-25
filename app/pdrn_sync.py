"""
SAP reports -> SQL Server, twice a day (13:00 and 17:00 server time). One engine, one feed per report:

  feed  SAP service (SICF, read-only)      report (T-code)                     SQL table (database PDRN)
  ----  ---------------------------------  ----------------------------------  -------------------------
  invr  /sap/zinvr  ZCL_INVR_JSON_HANDLER  ZSDR_UNIT (ZSD_INVR), units          SAP_INVR + SAP_INVR_LOAD
  pdrn  /sap/zpdrn  ZCL_PDRN_JSON_HANDLER  ZRSD_PROPERTY_DETAIL_REPORT          SAP_PDRN + SAP_PDRN_LOAD
                                           (ZSD_PDRN)

Every scheduled run loads INVR first, then PDRN, so both snapshots come from the same moment.

Per feed, every run:
  1. One service call per PROJECT (VG_<FEED>_WERKS) - small calls stay under SAP's ICM timeout.
  2. Checks: every project answers with rows; the status column holds only known values
     (PDRN BUKST = ACTIVE/CANCELLED; INVR VMSTB not blank); the row count (PDRN: active bookings)
     is >= VG_<FEED>_MIN_RATIO (0.9) of the last good load. Any failure -> the table is NOT touched
     (last good snapshot stays) and the reason is logged in <TABLE>_LOAD.
  3. Replaces the table in ONE transaction. Every report field is kept as text, same names as SAP
     (BUKRS, WNAME, MATNR / NAME1, MAKTX, VMSTB ...), plus LOAD_ID and LOADED_AT.

Routes (per feed key = pdrn | invr):
  /<feed>/health    last load, row count, next run, recent loads (no customer data)
  /<feed>/rows      the snapshot as JSON {asOn, loadId, count, rows} - this server only (127.0.0.1)
                    or header X-PDRN-Token = VG_PDRN_API_TOKEN
  /<feed>/refresh   load that feed now (same access rule)

Settings: environment (NSSM) or pdrn.env in the API folder (C:\\VendorGlobe_API\\pdrn.env, git-ignored;
values there win). Read on every run, so a new password needs no restart.
  Shared:  VG_PDRN_USER / VG_PDRN_PASSWORD (SAP login, REQUIRED), VG_PDRN_TIMES (13:00,17:00),
           VG_PDRN_DB_NAME (PDRN), VG_PDRN_VERIFY ("1" = verify TLS), VG_PDRN_TIMEOUT (1800 s),
           VG_PDRN_MIN_RATIO (0.9), VG_PDRN_CLIENT, VG_PDRN_API_TOKEN, VG_PDRN_RUN_ON_START (1)
  PDRN:    VG_PDRN_URL, VG_PDRN_BUKRS (1010,1070,2010,2070,3070), VG_PDRN_WERKS (8 projects),
           VG_PDRN_STATUS (all)
  INVR:    VG_INVR_ENABLED (0 - set 1 once /sap/zinvr is in Production), VG_INVR_URL,
           VG_INVR_CLIENT, VG_INVR_WERKS (8 projects), VG_INVR_TYPE (units),
           VG_INVR_USER / VG_INVR_PASSWORD (default: the PDRN login)

Standalone:  python app\\pdrn_sync.py --once [pdrn|invr]     (load now; default both)
             python app\\pdrn_sync.py --check [pdrn|invr]    (fetch + check, write nothing)
"""

import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db_config as cfg  # noqa: E402

try:
    import pyodbc
except Exception:  # the module still imports (health shows the error)
    pyodbc = None

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.environ.get("VG_PDRN_ENV_FILE") or os.path.join(ROOT, "pdrn.env")
SAP_PRD = "https://vhsmwps4ci.sap.smartworlddevelopers.com:20400"
PROJECTS = "1011,1013,1070,1072,2010,2071,3072,3073"
LOCAL_ADDRS = {"127.0.0.1", "::1", "localhost"}


def _load_env_file(path=ENV_FILE):
    """KEY=VALUE lines; '#' comments only at line start (passwords may contain '#')."""
    if not os.path.exists(path):
        return False
    with open(path, encoding="utf-8-sig") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                v = v[1:-1]
            if k:
                os.environ[k] = v
    return True


_load_env_file()


def _env(name, default=""):
    v = os.environ.get("VG_" + name)
    return default if v is None or v.strip() == "" else v.strip()


DB_NAME = _env("PDRN_DB_NAME", "PDRN")


def _log(msg):
    print(f"[pdrn_sync] {datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


# ---------------- feeds ----------------

class Feed:
    def __init__(self, key, report, path, table, status_field, allowed, count_where,
                 enabled_default):
        self.key, self.report, self.path = key, report, path
        self.env = key.upper()
        self.table, self.log_table = table, table + "_LOAD"
        self.status_field = status_field      # column checked on every row
        self.allowed = allowed                # set of allowed values, or None = any non-blank
        self.count_where = count_where        # (column, value) counted for the ratio check, None = all rows
        self.enabled_default = enabled_default
        self.lock = threading.Lock()
        self.status = {"configured": False, "running": False, "last_run": None, "last_ok": None,
                       "last_rows": None, "last_counted": None, "last_error": None,
                       "next_run": None, "progress": None}

    def settings(self):
        _load_env_file()
        e = self.env

        def g(n, d=""):                       # feed value, else the shared PDRN value
            return _env(f"{e}_{n}", _env(f"PDRN_{n}", d))

        s = {
            "enabled": _env(f"{e}_ENABLED", "1" if self.enabled_default else "0") == "1",
            "user": g("USER"),
            "password": os.environ.get(f"VG_{e}_PASSWORD") or os.environ.get("VG_PDRN_PASSWORD", ""),
            "url": _env(f"{e}_URL", SAP_PRD + self.path),
            "client": g("CLIENT"),
            "werks": _env(f"{e}_WERKS", PROJECTS),
            "verify": g("VERIFY", "0") == "1",
            "timeout": int(g("TIMEOUT", "1800")),
            "min_ratio": float(g("MIN_RATIO", "0.9")),
            "times": [t.strip() for t in _env("PDRN_TIMES", "13:00,17:00").split(",") if t.strip()],
        }
        if self.key == "pdrn":
            s["extra"] = {"bukrs": _env("PDRN_BUKRS", "1010,1070,2010,2070,3070"),
                          "status": _env("PDRN_STATUS", "all")}
        else:
            s["extra"] = {"type": _env("INVR_TYPE", "units")}
        return s

    def counted(self, rows):
        if self.count_where is None:
            return len(rows)
        col, val = self.count_where
        return sum(1 for r in rows if str(r.get(col) or "").strip() == val)


FEEDS = {
    "invr": Feed("invr", "ZSD_INVR", "/sap/zinvr", "SAP_INVR", "VMSTB", None, None, False),
    "pdrn": Feed("pdrn", "ZSD_PDRN", "/sap/zpdrn", "SAP_PDRN", "BUKST", {"ACTIVE", "CANCELLED"},
                 ("BUKST", "ACTIVE"), True),
}
ORDER = ["invr", "pdrn"]          # INVR first, then PDRN
STATUS = FEEDS["pdrn"].status     # kept for older callers


# ---------------- SQL ----------------

def _conn(database=None, autocommit=True):
    if pyodbc is None:
        raise RuntimeError("pyodbc is not installed")
    return pyodbc.connect(
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};DATABASE={database or DB_NAME};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;", autocommit=autocommit)


def _sanitize(name):
    n = re.sub(r"[^0-9A-Za-z_]", "_", str(name)).strip("_") or "col"
    return ("c_" + n if n[0].isdigit() else n)[:120].upper()


def ensure_schema(f):
    c = _conn("master")
    try:
        c.cursor().execute("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = ?) "
                           "EXEC('CREATE DATABASE [' + ? + ']')", DB_NAME, DB_NAME)
    finally:
        c.close()
    c = _conn()
    try:
        cur = c.cursor()
        cur.execute(f"IF OBJECT_ID('dbo.{f.table}') IS NULL "
                    f"CREATE TABLE dbo.{f.table} (LOAD_ID INT NOT NULL, LOADED_AT DATETIME2(0) NOT NULL)")
        cur.execute(f"IF OBJECT_ID('dbo.{f.log_table}') IS NULL "
                    f"CREATE TABLE dbo.{f.log_table} (LOAD_ID INT IDENTITY(1,1) PRIMARY KEY, "
                    f"STARTED_AT DATETIME2(0) NOT NULL, FINISHED_AT DATETIME2(0) NULL, OK BIT NULL, "
                    f"ROWS_N INT NULL, ACTIVE_N INT NULL, COMPANIES NVARCHAR(400) NULL, "
                    f"TRIGGERED_BY NVARCHAR(40) NULL, MESSAGE NVARCHAR(MAX) NULL)")
    finally:
        c.close()


def _columns(cur, table):
    cur.execute("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?)", f"dbo.{table}")
    return {r[0].upper() for r in cur.fetchall()}


def _current_count(f):
    try:
        c = _conn()
        try:
            cur = c.cursor()
            cols = _columns(cur, f.table)
            if f.count_where is None:
                if len(cols) <= 2:
                    return 0
                cur.execute(f"SELECT COUNT(*) FROM dbo.{f.table}")
            else:
                col, val = f.count_where
                if col not in cols:
                    return 0
                cur.execute(f"SELECT COUNT(*) FROM dbo.{f.table} WHERE [{col}] = ?", val)
            return int(cur.fetchone()[0])
        finally:
            c.close()
    except Exception:
        return 0


def _sql_value(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "X" if v else ""
    if isinstance(v, float) and v == int(v):
        v = int(v)
    s = str(v).strip()
    return s[:4000] if s != "" else None


def _write_snapshot(f, load_id, loaded_at, rows):
    fields = []
    for r in rows:                      # union of fields, first-seen order
        for k in r:
            if k not in fields:
                fields.append(k)
    cols = [_sanitize(x) for x in fields]
    c = _conn(autocommit=False)
    try:
        cur = c.cursor()
        have = _columns(cur, f.table)
        for col in cols:
            if col not in have:
                cur.execute(f"ALTER TABLE dbo.{f.table} ADD [{col}] NVARCHAR(4000) NULL")
        c.commit()
        cur.execute(f"DELETE FROM dbo.{f.table}")
        sql = (f"INSERT INTO dbo.{f.table} (LOAD_ID, LOADED_AT, " + ", ".join(f"[{x}]" for x in cols) +
               ") VALUES (?, ?, " + ", ".join("?" for _ in cols) + ")")
        cur.fast_executemany = True
        data = [[load_id, loaded_at] + [_sql_value(r.get(x)) for x in fields] for r in rows]
        for i in range(0, len(data), 1000):
            cur.executemany(sql, data[i:i + 1000])
        c.commit()                      # old snapshot replaced only here
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def _log_start(f, trigger, scope):
    c = _conn()
    try:
        cur = c.cursor()
        cur.execute(f"INSERT INTO dbo.{f.log_table} (STARTED_AT, COMPANIES, TRIGGERED_BY) "
                    f"OUTPUT INSERTED.LOAD_ID VALUES (SYSDATETIME(), ?, ?)", scope[:400], trigger)
        return int(cur.fetchone()[0])
    finally:
        c.close()


def _log_end(f, load_id, ok, rows_n, counted, message):
    c = _conn()
    try:
        c.cursor().execute(f"UPDATE dbo.{f.log_table} SET FINISHED_AT = SYSDATETIME(), OK = ?, "
                           f"ROWS_N = ?, ACTIVE_N = ?, MESSAGE = ? WHERE LOAD_ID = ?",
                           1 if ok else 0, rows_n, counted, (message or "")[:4000], load_id)
    finally:
        c.close()


# ---------------- SAP ----------------

def _fetch(f, s, werks):
    """One service call for one project. Returns (rows with UPPER-case keys, seconds)."""
    params = dict(s["extra"], saml2="disabled")
    if werks:
        params["werks"] = werks
    if s["client"]:
        params["sap-client"] = s["client"]
    if not s["verify"]:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    t0 = time.time()
    r = requests.get(s["url"], params=params, auth=(s["user"], s["password"]), verify=s["verify"],
                     timeout=s["timeout"], headers={"Accept": "application/json"})
    secs = time.time() - t0
    if r.status_code == 401:
        raise RuntimeError("401 - SAP rejected the user/password (VG_PDRN_USER / VG_PDRN_PASSWORD)")
    if r.status_code == 403:
        raise RuntimeError(f"403 - user has no {f.report} authorization")
    if r.status_code == 404:
        raise RuntimeError(f"404 - {f.path} not found or not active in SICF")
    if r.status_code != 200:
        # "500 Connection Timed Out" = SAP's ICM gave up; raise PROCTIMEOUT on the port
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    if r.text.lstrip()[:1] != "{":
        raise RuntimeError("SAP returned a web page instead of JSON (login page?)")
    ans = r.json()
    rows = ans.get("rows", ans.get("ROWS"))
    if not isinstance(rows, list):
        raise RuntimeError(f"project {werks}: answer has no rows")
    cnt = ans.get("count", ans.get("COUNT"))
    if cnt is not None and int(cnt) != len(rows):
        raise RuntimeError(f"project {werks}: count {cnt} but {len(rows)} rows arrived")
    return [{str(k).upper(): v for k, v in x.items()} for x in rows], secs


def run_feed(key, trigger="manual", check_only=False):
    """Fetch one feed for every project, check, and (unless check_only) replace its SQL snapshot.
    Returns a summary dict; raises nothing - errors go to the feed status / <TABLE>_LOAD."""
    f = FEEDS[key]
    if not f.lock.acquire(blocking=False):
        return {"ok": False, "message": f"{key}: a load is already running"}
    s = f.settings()
    st = f.status
    st.update(running=True, last_run=datetime.now().isoformat(timespec="seconds"))
    load_id, rows_all, counted = None, [], None
    try:
        if not s["user"] or not s["password"]:
            raise RuntimeError("VG_PDRN_USER / VG_PDRN_PASSWORD not set (pdrn.env in the API folder)")
        projects = ([w.strip() for w in s["werks"].split(",") if w.strip()]
                    if s["werks"] != "*" else [None])
        if not check_only:
            ensure_schema(f)
            load_id = _log_start(f, trigger, s["werks"])
        parts = []
        for i, w in enumerate(projects, 1):
            label = f"project {w}" if w else "all projects"
            st["progress"] = f"{label} ({i}/{len(projects)})"
            rows, secs = _fetch(f, s, w)
            if not rows:
                raise RuntimeError(f"{label}: SAP returned 0 rows - not loading")
            parts.append(f"{w or '*'}:{len(rows)} ({secs:.0f}s)")
            _log(f"{key} {label}: {len(rows)} rows in {secs:.0f}s")
            rows_all.extend(rows)
        vals = {str(r.get(f.status_field) or "").strip() for r in rows_all}
        bad = (vals - f.allowed) if f.allowed is not None else ({""} & vals)
        if bad:
            raise RuntimeError(f"unexpected {f.status_field} values {sorted(bad)} - not loading")
        counted = f.counted(rows_all)
        old = _current_count(f)
        if old and counted < old * s["min_ratio"]:
            raise RuntimeError(f"{'active bookings' if f.count_where else 'rows'} fell from {old} to "
                               f"{counted} (below {s['min_ratio']:.0%}) - looks incomplete, not loading")
        msg = (f"{len(rows_all)} rows" + (f", {counted} active" if f.count_where else "") +
               " · " + ", ".join(parts))
        if check_only:
            return {"ok": True, "message": f"{key} check only - " + msg}
        loaded_at = datetime.now().replace(microsecond=0)
        _write_snapshot(f, load_id, loaded_at, rows_all)
        _log_end(f, load_id, True, len(rows_all), counted, msg)
        st.update(last_ok=loaded_at.isoformat(), last_rows=len(rows_all), last_counted=counted,
                  last_error=None)
        _log(f"{key} loaded " + msg)
        return {"ok": True, "loadId": load_id, "message": msg}
    except Exception as e:
        err = str(e)
        st.update(last_error=f"{datetime.now():%Y-%m-%d %H:%M} {err}")
        _log(f"{key} FAILED - table not changed: " + err)
        if load_id is not None:
            try:
                _log_end(f, load_id, False, len(rows_all), counted, err)
            except Exception:
                pass
        return {"ok": False, "message": err}
    finally:
        st["running"] = False
        st["progress"] = None
        f.lock.release()


def run_once(trigger="manual", check_only=False):
    """Kept for older callers: the PDRN feed."""
    return run_feed("pdrn", trigger, check_only)


def run_all(trigger="manual", check_only=False, only=None):
    out = {}
    for key in ORDER:
        if only and key not in only:
            continue
        if not FEEDS[key].settings()["enabled"]:
            out[key] = {"ok": None, "message": "disabled"}
            continue
        out[key] = run_feed(key, trigger, check_only)
    return out


# ---------------- schedule ----------------

def _next_run(times, now=None):
    now = now or datetime.now()
    cands = []
    for t in times:
        try:
            h, m = [int(x) for x in t.split(":")]
        except ValueError:
            continue
        d = now.replace(hour=h, minute=m, second=0, microsecond=0)
        cands.append(d if d > now else d + timedelta(days=1))
    return min(cands) if cands else now + timedelta(hours=12)


def _table_empty(f):
    ensure_schema(f)
    c = _conn()
    try:
        return c.cursor().execute(f"SELECT COUNT(*) FROM dbo.{f.table}").fetchone()[0] == 0
    finally:
        c.close()


def _loop():
    if _env("PDRN_RUN_ON_START", "1") == "1":
        for key in ORDER:
            f = FEEDS[key]
            s = f.settings()
            if not (s["enabled"] and s["user"]):
                continue
            try:
                if _table_empty(f):
                    _log(f"{key}: table empty - first load now")
                    run_feed(key, "startup")
            except Exception as e:
                _log(f"{key}: startup check failed: {e}")
    while True:
        nxt = _next_run(FEEDS["pdrn"].settings()["times"])
        for f in FEEDS.values():
            f.status["next_run"] = nxt.isoformat(timespec="minutes")
        while datetime.now() < nxt:
            time.sleep(min(60, max(1, (nxt - datetime.now()).total_seconds())))
        run_all("schedule")


def start_background_thread():
    for key in ORDER:
        f = FEEDS[key]
        s = f.settings()
        f.status["configured"] = bool(s["user"] and s["password"])
        _log(f"{key}: {'ENABLED' if s['enabled'] else 'disabled'} -> "
             f"{cfg.DB_SERVER}/{DB_NAME}.dbo.{f.table} from {s['url']}")
    s = FEEDS["pdrn"].settings()
    if not (s["user"] and s["password"]):
        _log("VG_PDRN_USER / VG_PDRN_PASSWORD not set - scheduler idle (fill pdrn.env, restart)")
    _log(f"scheduled at {', '.join(s['times'])} (order: {' -> '.join(ORDER)})")
    threading.Thread(target=_loop, name="pdrn_sync", daemon=True).start()


# ---------------- routes ----------------

def register(app):
    from flask import jsonify, request

    def _allowed():
        tok = _env("PDRN_API_TOKEN")
        return request.remote_addr in LOCAL_ADDRS or (tok and request.headers.get("X-PDRN-Token") == tok)

    def _health(key):
        f = FEEDS[key]
        s = f.settings()
        out = dict(f.status)
        out.update(feed=key, report=f.report, enabled=s["enabled"], source=s["url"],
                   table=f"{DB_NAME}.dbo.{f.table}", times=s["times"], projects=s["werks"],
                   companies=s["extra"]["bukrs"].split(",") if key == "pdrn" else None)
        try:
            c = _conn()
            try:
                cur = c.cursor()
                cur.execute(f"IF OBJECT_ID('dbo.{f.table}') IS NOT NULL "
                            f"SELECT TOP 1 LOAD_ID, LOADED_AT, COUNT(*) OVER () FROM dbo.{f.table}")
                r = cur.fetchone() if cur.description else None
                out["snapshot"] = ({"loadId": r[0], "loadedAt": r[1].isoformat(), "rows": r[2]}
                                   if r else None)
                cur.execute(f"IF OBJECT_ID('dbo.{f.log_table}') IS NOT NULL "
                            f"SELECT TOP 5 LOAD_ID, STARTED_AT, FINISHED_AT, OK, ROWS_N, ACTIVE_N, "
                            f"TRIGGERED_BY, MESSAGE FROM dbo.{f.log_table} ORDER BY LOAD_ID DESC")
                rows = cur.fetchall() if cur.description else []
                out["recentLoads"] = [{"loadId": a, "started": b and b.isoformat(),
                                       "finished": fi and fi.isoformat(),
                                       "ok": None if o is None else bool(o),
                                       "rows": n, "active": act, "by": by, "message": m}
                                      for a, b, fi, o, n, act, by, m in rows]
            finally:
                c.close()
        except Exception as e:
            out["sqlError"] = str(e)
        return jsonify(out)

    def _rows(key):
        if not _allowed():
            return jsonify({"error": "only from this server (or X-PDRN-Token)"}), 403
        f = FEEDS[key]
        c = _conn()
        try:
            cur = c.cursor()
            cur.execute(f"IF OBJECT_ID('dbo.{f.table}') IS NOT NULL SELECT * FROM dbo.{f.table}")
            rows, as_on, load_id = [], None, None
            if cur.description:
                names = [d[0] for d in cur.description]
                for rec in cur.fetchall():
                    d = dict(zip(names, rec))
                    load_id, as_on = d.pop("LOAD_ID"), d.pop("LOADED_AT")
                    rows.append(d)
        finally:
            c.close()
        return jsonify({"feed": key, "asOn": as_on.isoformat() if as_on else None,
                        "loadId": load_id, "count": len(rows), "rows": rows})

    def _refresh(key):
        if not _allowed():
            return jsonify({"error": "only from this server (or X-PDRN-Token)"}), 403
        if FEEDS[key].status["running"]:
            return jsonify({"ok": False, "message": f"{key}: a load is already running"}), 409
        threading.Thread(target=run_feed, args=(key, "api"), daemon=True).start()
        return jsonify({"ok": True, "message": f"{key} load started - see /{key}/health"})

    for key in FEEDS:
        app.add_url_rule(f"/{key}/health", f"{key}_health", lambda key=key: _health(key))
        app.add_url_rule(f"/{key}/rows", f"{key}_rows", lambda key=key: _rows(key))
        app.add_url_rule(f"/{key}/refresh", f"{key}_refresh", lambda key=key: _refresh(key),
                         methods=["GET", "POST"])


if __name__ == "__main__":
    only = [a for a in sys.argv[1:] if a in FEEDS] or None
    if "--check" in sys.argv:
        print(run_all("cli", check_only=True, only=only))
    elif "--once" in sys.argv:
        print(run_all("cli", only=only))
    else:
        print(__doc__)
