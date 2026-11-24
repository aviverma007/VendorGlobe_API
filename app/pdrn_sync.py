"""
PDRN (ZSD_PDRN bookings report) from SAP Production -> SQL Server, twice a day.

SAP side: read-only SICF service /sap/zpdrn (class ZCL_PDRN_JSON_HANDLER, transport DS4K913636),
which runs report ZRSD_PROPERTY_DETAIL_REPORT and returns its ALV table (143 fields) as JSON.

Every run (default 13:00 and 17:00 server time):
  1. GET /sap/zpdrn once per company (status=all, the report's "All" radio button).
  2. Checks: every company answers with rows, Booking Status (BUKST) is ACTIVE / CANCELLED only,
     active bookings >= VG_PDRN_MIN_RATIO of the last good load.  Any failure -> the table is NOT
     touched (the last good snapshot stays) and the reason is logged in SAP_PDRN_LOAD.
  3. Replaces dbo.SAP_PDRN with the new snapshot in ONE transaction (readers never see half a load).
     Every SAP field is kept, as text exactly as SAP sent it (same names: BUKRS, WNAME, MATNR ...).

Tables (database VG_PDRN_DB_NAME, default PDRN - created automatically):
  dbo.SAP_PDRN        current snapshot: all ZSD_PDRN fields + LOAD_ID, LOADED_AT
  dbo.SAP_PDRN_LOAD   one row per run: started/finished, ok, rows, active, message

Routes:
  /pdrn/health    last load, row count, next run, last error (no customer data)
  /pdrn/rows      the snapshot as JSON {asOn, loadId, count, rows}  - this server only
                  (127.0.0.1) or header X-PDRN-Token = VG_PDRN_API_TOKEN
  /pdrn/refresh   run a load now (same access rule)

Settings: environment variables (NSSM) or pdrn.env in the API folder (C:\\VendorGlobe_API\\pdrn.env,
git-ignored; values there win):
  VG_PDRN_USER / VG_PDRN_PASSWORD   SAP Production login            REQUIRED
  VG_PDRN_TIMES      default 13:00,17:00
  VG_PDRN_URL        default https://vhsmwps4ci.sap.smartworlddevelopers.com:20400/sap/zpdrn
  VG_PDRN_CLIENT     sap-client (default: system default)
  VG_PDRN_BUKRS      default 1010,1070,2010,2070,3070
  VG_PDRN_WERKS      default 1011,1013,1070,1072,2010,2071,3072,3073   ("*" = all projects)
  VG_PDRN_STATUS     all | book | inproc (default all)
  VG_PDRN_VERIFY     "1" = verify TLS (default off: SAP internal certificate)
  VG_PDRN_TIMEOUT    seconds per company (default 1800)
  VG_PDRN_MIN_RATIO  default 0.9
  VG_PDRN_DB_NAME    default PDRN
  VG_PDRN_API_TOKEN  optional, lets another machine read /pdrn/rows
  VG_PDRN_RUN_ON_START  "1" = load at service start when the table is empty (default 1)

Standalone:  python app\\pdrn_sync.py --once     (load now)
             python app\\pdrn_sync.py --check    (fetch + check, write nothing)
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


DEFAULT_URL = "https://vhsmwps4ci.sap.smartworlddevelopers.com:20400/sap/zpdrn"
DB_NAME = _env("PDRN_DB_NAME", "PDRN")
TABLE, LOG_TABLE = "SAP_PDRN", "SAP_PDRN_LOAD"
LOCAL_ADDRS = {"127.0.0.1", "::1", "localhost"}

_lock = threading.Lock()
STATUS = {"configured": False, "running": False, "last_run": None, "last_ok": None,
          "last_rows": None, "last_active": None, "last_error": None, "next_run": None,
          "companies": None}


def _log(msg):
    print(f"[pdrn_sync] {datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


def _settings():
    """Read on every run, so a changed pdrn.env needs no service restart."""
    _load_env_file()
    return {
        "user": _env("PDRN_USER"), "password": os.environ.get("VG_PDRN_PASSWORD", ""),
        "url": _env("PDRN_URL", DEFAULT_URL), "client": _env("PDRN_CLIENT"),
        "bukrs": [b.strip() for b in _env("PDRN_BUKRS", "1010,1070,2010,2070,3070").split(",") if b.strip()],
        "werks": _env("PDRN_WERKS", "1011,1013,1070,1072,2010,2071,3072,3073"),
        "status": _env("PDRN_STATUS", "all"), "verify": _env("PDRN_VERIFY", "0") == "1",
        "timeout": int(_env("PDRN_TIMEOUT", "1800")),
        "min_ratio": float(_env("PDRN_MIN_RATIO", "0.9")),
        "times": [t.strip() for t in _env("PDRN_TIMES", "13:00,17:00").split(",") if t.strip()],
        "token": _env("PDRN_API_TOKEN"),
    }


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


def ensure_schema():
    c = _conn("master")
    try:
        c.cursor().execute("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = ?) "
                           "EXEC('CREATE DATABASE [' + ? + ']')", DB_NAME, DB_NAME)
    finally:
        c.close()
    c = _conn()
    try:
        cur = c.cursor()
        cur.execute(f"IF OBJECT_ID('dbo.{TABLE}') IS NULL "
                    f"CREATE TABLE dbo.{TABLE} (LOAD_ID INT NOT NULL, LOADED_AT DATETIME2(0) NOT NULL)")
        cur.execute(f"IF OBJECT_ID('dbo.{LOG_TABLE}') IS NULL "
                    f"CREATE TABLE dbo.{LOG_TABLE} (LOAD_ID INT IDENTITY(1,1) PRIMARY KEY, "
                    f"STARTED_AT DATETIME2(0) NOT NULL, FINISHED_AT DATETIME2(0) NULL, OK BIT NULL, "
                    f"ROWS_N INT NULL, ACTIVE_N INT NULL, COMPANIES NVARCHAR(400) NULL, "
                    f"TRIGGERED_BY NVARCHAR(40) NULL, MESSAGE NVARCHAR(MAX) NULL)")
    finally:
        c.close()


def _columns(cur):
    cur.execute("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?)", f"dbo.{TABLE}")
    return {r[0].upper() for r in cur.fetchall()}


def _current_active():
    try:
        c = _conn()
        try:
            cur = c.cursor()
            if "BUKST" not in _columns(cur):
                return 0
            cur.execute(f"SELECT COUNT(*) FROM dbo.{TABLE} WHERE BUKST = 'ACTIVE'")
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


def _write_snapshot(load_id, loaded_at, rows):
    fields = []
    for r in rows:                      # union of fields, first-seen order
        for k in r:
            if k not in fields:
                fields.append(k)
    cols = [_sanitize(f) for f in fields]
    c = _conn(autocommit=False)
    try:
        cur = c.cursor()
        have = _columns(cur)
        for col in cols:
            if col not in have:
                cur.execute(f"ALTER TABLE dbo.{TABLE} ADD [{col}] NVARCHAR(4000) NULL")
        c.commit()
        cur.execute(f"DELETE FROM dbo.{TABLE}")
        sql = (f"INSERT INTO dbo.{TABLE} (LOAD_ID, LOADED_AT, " + ", ".join(f"[{x}]" for x in cols) +
               ") VALUES (?, ?, " + ", ".join("?" for _ in cols) + ")")
        cur.fast_executemany = True
        data = [[load_id, loaded_at] + [_sql_value(r.get(f)) for f in fields] for r in rows]
        for i in range(0, len(data), 1000):
            cur.executemany(sql, data[i:i + 1000])
        c.commit()                      # old snapshot replaced only here
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def _log_start(trigger, companies):
    c = _conn()
    try:
        cur = c.cursor()
        cur.execute(f"INSERT INTO dbo.{LOG_TABLE} (STARTED_AT, COMPANIES, TRIGGERED_BY) "
                    f"OUTPUT INSERTED.LOAD_ID VALUES (SYSDATETIME(), ?, ?)", ",".join(companies), trigger)
        return int(cur.fetchone()[0])
    finally:
        c.close()


def _log_end(load_id, ok, rows_n, active_n, message):
    c = _conn()
    try:
        c.cursor().execute(f"UPDATE dbo.{LOG_TABLE} SET FINISHED_AT = SYSDATETIME(), OK = ?, ROWS_N = ?, "
                           f"ACTIVE_N = ?, MESSAGE = ? WHERE LOAD_ID = ?",
                           1 if ok else 0, rows_n, active_n, (message or "")[:4000], load_id)
    finally:
        c.close()


# ---------------- SAP ----------------

def _fetch_company(s, bukrs):
    params = {"bukrs": bukrs, "status": s["status"], "saml2": "disabled"}
    if s["werks"] != "*":
        params["werks"] = s["werks"]
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
        raise RuntimeError("403 - user has no ZSD_PDRN authorization in Production")
    if r.status_code == 404:
        raise RuntimeError("404 - /sap/zpdrn not found or not active in SICF")
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    if r.text.lstrip()[:1] != "{":
        raise RuntimeError("SAP returned a web page instead of JSON (login page?)")
    ans = r.json()
    rows = ans.get("rows", ans.get("ROWS"))
    if not isinstance(rows, list):
        raise RuntimeError(f"company {bukrs}: answer has no rows")
    cnt = ans.get("count", ans.get("COUNT"))
    if cnt is not None and int(cnt) != len(rows):
        raise RuntimeError(f"company {bukrs}: count {cnt} but {len(rows)} rows arrived")
    rows = [{str(k).upper(): v for k, v in x.items()} for x in rows]
    return rows, secs


def run_once(trigger="manual", check_only=False):
    """Fetch every company, check, and (unless check_only) replace the SQL snapshot.
    Returns a summary dict; raises nothing - errors go to STATUS / SAP_PDRN_LOAD."""
    if not _lock.acquire(blocking=False):
        return {"ok": False, "message": "a load is already running"}
    s = _settings()
    STATUS.update(running=True, last_run=datetime.now().isoformat(timespec="seconds"),
                  companies=s["bukrs"])
    load_id = None
    rows_all, active = [], None
    try:
        if not s["user"] or not s["password"]:
            raise RuntimeError("VG_PDRN_USER / VG_PDRN_PASSWORD not set (pdrn.env in the API folder)")
        if not check_only:
            ensure_schema()
            load_id = _log_start(trigger, s["bukrs"])
        parts = []
        for b in s["bukrs"]:
            rows, secs = _fetch_company(s, b)
            if not rows:
                raise RuntimeError(f"company {b}: SAP returned 0 rows - not loading")
            parts.append(f"{b}:{len(rows)} ({secs:.0f}s)")
            _log(f"company {b}: {len(rows)} rows in {secs:.0f}s")
            rows_all.extend(rows)
        bad = {str(r.get("BUKST") or "").strip() for r in rows_all} - {"ACTIVE", "CANCELLED"}
        if bad:
            raise RuntimeError(f"unexpected Booking Status values {sorted(bad)} - not loading")
        active = sum(1 for r in rows_all if str(r.get("BUKST")).strip() == "ACTIVE")
        old = _current_active()
        if old and active < old * s["min_ratio"]:
            raise RuntimeError(f"active bookings fell from {old} to {active} "
                               f"(below {s['min_ratio']:.0%}) - looks incomplete, not loading")
        msg = f"{len(rows_all)} rows, {active} active · " + ", ".join(parts)
        if check_only:
            return {"ok": True, "message": "check only - " + msg}
        loaded_at = datetime.now().replace(microsecond=0)
        _write_snapshot(load_id, loaded_at, rows_all)
        _log_end(load_id, True, len(rows_all), active, msg)
        STATUS.update(last_ok=loaded_at.isoformat(), last_rows=len(rows_all), last_active=active,
                      last_error=None)
        _log("loaded " + msg)
        return {"ok": True, "loadId": load_id, "message": msg}
    except Exception as e:
        err = str(e)
        STATUS.update(last_error=f"{datetime.now():%Y-%m-%d %H:%M} {err}")
        _log("FAILED - table not changed: " + err)
        if load_id is not None:
            try:
                _log_end(load_id, False, len(rows_all), active, err)
            except Exception:
                pass
        return {"ok": False, "message": err}
    finally:
        STATUS["running"] = False
        _lock.release()


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


def _loop():
    s = _settings()
    if _env("PDRN_RUN_ON_START", "1") == "1" and s["user"]:
        try:
            ensure_schema()
            c = _conn()
            try:
                n = c.cursor().execute(f"SELECT COUNT(*) FROM dbo.{TABLE}").fetchone()[0]
            finally:
                c.close()
            if n == 0:
                _log("table empty - first load now")
                run_once("startup")
        except Exception as e:
            _log(f"startup check failed: {e}")
    while True:
        s = _settings()
        nxt = _next_run(s["times"])
        STATUS["next_run"] = nxt.isoformat(timespec="minutes")
        while datetime.now() < nxt:
            time.sleep(min(60, max(1, (nxt - datetime.now()).total_seconds())))
        run_once("schedule")


def start_background_thread():
    s = _settings()
    STATUS["configured"] = bool(s["user"] and s["password"])
    if not STATUS["configured"]:
        _log("VG_PDRN_USER / VG_PDRN_PASSWORD not set - scheduler idle (fill pdrn.env, restart)")
    _log(f"scheduled at {', '.join(s['times'])} -> {cfg.DB_SERVER}/{DB_NAME}.dbo.{TABLE}")
    threading.Thread(target=_loop, name="pdrn_sync", daemon=True).start()


# ---------------- routes ----------------

def register(app):
    from flask import jsonify, request

    def _allowed():
        tok = _settings()["token"]
        return request.remote_addr in LOCAL_ADDRS or (tok and request.headers.get("X-PDRN-Token") == tok)

    @app.route("/pdrn/health")
    def pdrn_health():
        out = dict(STATUS)
        out.update(table=f"{DB_NAME}.dbo.{TABLE}", times=_settings()["times"])
        try:
            c = _conn()
            try:
                cur = c.cursor()
                cur.execute(f"SELECT TOP 1 LOAD_ID, LOADED_AT, COUNT(*) OVER () FROM dbo.{TABLE}")
                r = cur.fetchone()
                out["snapshot"] = ({"loadId": r[0], "loadedAt": r[1].isoformat(), "rows": r[2]}
                                   if r else None)
                cur.execute(f"SELECT TOP 5 LOAD_ID, STARTED_AT, FINISHED_AT, OK, ROWS_N, ACTIVE_N, "
                            f"TRIGGERED_BY, MESSAGE FROM dbo.{LOG_TABLE} ORDER BY LOAD_ID DESC")
                out["recentLoads"] = [{"loadId": a, "started": b and b.isoformat(),
                                       "finished": f and f.isoformat(), "ok": None if o is None else bool(o),
                                       "rows": n, "active": act, "by": by, "message": m}
                                      for a, b, f, o, n, act, by, m in cur.fetchall()]
            finally:
                c.close()
        except Exception as e:
            out["sqlError"] = str(e)
        return jsonify(out)

    @app.route("/pdrn/rows")
    def pdrn_rows():
        if not _allowed():
            return jsonify({"error": "only from this server (or X-PDRN-Token)"}), 403
        c = _conn()
        try:
            cur = c.cursor()
            cur.execute(f"SELECT * FROM dbo.{TABLE}")
            names = [d[0] for d in cur.description]
            rows, as_on, load_id = [], None, None
            for rec in cur.fetchall():
                d = dict(zip(names, rec))
                load_id, as_on = d.pop("LOAD_ID"), d.pop("LOADED_AT")
                rows.append(d)
        finally:
            c.close()
        return jsonify({"asOn": as_on.isoformat() if as_on else None, "loadId": load_id,
                        "count": len(rows), "rows": rows})

    @app.route("/pdrn/refresh", methods=["GET", "POST"])
    def pdrn_refresh():
        if not _allowed():
            return jsonify({"error": "only from this server (or X-PDRN-Token)"}), 403
        if STATUS["running"]:
            return jsonify({"ok": False, "message": "a load is already running"}), 409
        threading.Thread(target=run_once, args=("api",), daemon=True).start()
        return jsonify({"ok": True, "message": "load started - see /pdrn/health"})


if __name__ == "__main__":
    if "--check" in sys.argv:
        print(run_once("cli", check_only=True))
    elif "--once" in sys.argv:
        print(run_once("cli"))
    else:
        print(__doc__)
