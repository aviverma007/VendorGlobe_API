"""
SAP reports -> SQL Server, twice a day (13:00 and 17:00 server time). One engine, one feed per report:

  feed  SAP service (SICF, read-only)      report (T-code)                     SQL table (database PDRN)
  ----  ---------------------------------  ----------------------------------  -------------------------
  invr  /sap/zinvr  ZCL_INVR_JSON_HANDLER  ZSDR_UNIT (ZSD_INVR), units          SAP_INVR + SAP_INVR_LOAD
  pdrn  /sap/zpdrn  ZCL_PDRN_JSON_HANDLER  ZRSD_PROPERTY_DETAIL_REPORT          SAP_PDRN + SAP_PDRN_LOAD
                                           (ZSD_PDRN)
  zrec     /sap/zrec   ZCL_ZREC_JSON_HANDLER  ZSD_REC_PRINT_NEW (ZREC_PRINT)     SAP_ZREC    (per company
                                           per calendar year - CALL_KEY "1010 2024")
  zdapp    /sap/zdapp  ZCL_DAPP_JSON_HANDLER  ZSDR_DUE_PAYMENT (ZSD_DAPP)        SAP_ZDAPP   (per company,
                                           Due and Undue separately - CALL_KEY "1010 due")
  zalr_np  /sap/zalr   ZCL_ZALR_JSON_HANDLER  ZALR_PROG2 (ZALR) Non-Project      SAP_ZALR_NP (per view x 5
                                           definitions: Item Wise PO, Summary, Total Actual - current FY;
                                           CALL_KEY "np_po HO1000-RE1060")
  zalr_pj  /sap/zalr   ZCL_ZALR_JSON_HANDLER  ZALR_PROG2 (ZALR) Project          SAP_ZALR_PJ (per Project
                                           definition from view=projlist, project to date)
  Every row also carries CALL_KEY = the company / view / project definition of the call it came from.

Every scheduled run loads INVR first, then PDRN, so both snapshots come from the same moment.

Per feed, every run:
  1. One service call per PROJECT (VG_<FEED>_WERKS) - small calls stay under SAP's ICM timeout.
  2. Checks: every project answers with rows; the status column holds only known values
     (PDRN BUKST = ACTIVE/CANCELLED; INVR VMSTB not blank); the row count (PDRN: active bookings)
     is >= VG_<FEED>_MIN_RATIO (0.9) of the last good load. Any failure -> the table is NOT touched
     (last good snapshot stays) and the reason is logged in <TABLE>_LOAD.
  3. Replaces the table in ONE transaction. Every report field is kept as text, same names as SAP
     (BUKRS, WNAME, MATNR / NAME1, MAKTX, VMSTB ...), plus LOAD_ID and LOADED_AT.

Routes (per feed key = pdrn | invr | zrec | zdapp | zalr_np | zalr_pj):
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
           VG_INVR_CLIENT, VG_INVR_WERKS (8 projects), VG_INVR_TYPE (units,transit),
           VG_INVR_USER / VG_INVR_PASSWORD (default: the PDRN login)
  ZREC:    VG_ZREC_ENABLED (1), VG_ZREC_BUKRS (1010,1070,2010,2070,3070), VG_ZREC_FROM (2015-01-01)
  ZDAPP:   VG_ZDAPP_ENABLED (1), VG_ZDAPP_BUKRS (same 5 companies), VG_ZDAPP_VIEWS (due,undue)
  ZALR_NP: VG_ZALR_NP_ENABLED (1), VG_ZALR_NP_VIEWS (np_po,np_summary,np_total_actual),
           VG_ZALR_NP_PSPHI (the 46 Non-Project definitions in NP_DEFS), VG_ZALR_NP_BATCH (5 per call),
           VG_ZALR_NP_WBS (*F26* = WBS of the current fiscal year, moves on every April),
           VG_ZALR_NP_YEARS (optional extra posting-year filter, off by default)
  ZALR_PJ: VG_ZALR_PJ_ENABLED (1), VG_ZALR_PJ_PSPHI (* = every Project definition except the
           Non-Project ones above, or a comma list), VG_ZALR_PJ_EXTRA (RE0111 - projects that carry
           the Non-Project budget profile in PRD, always included)
  Any feed: VG_<FEED>_URL / _CLIENT / _TIMEOUT / _MIN_RATIO / _USER / _PASSWORD override the shared value.

Standalone:  python app\\pdrn_sync.py --once [feed ...]     (load now; default all enabled)
             python app\\pdrn_sync.py --check [feed ...]    (fetch + check, write nothing)
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


_FROM_FILE = set()      # keys that came from pdrn.env (a line deleted there is dropped again)


def _load_env_file(path=ENV_FILE):
    """KEY=VALUE lines; '#' comments only at line start (passwords may contain '#')."""
    if not os.path.exists(path):
        return False
    seen = set()
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
                seen.add(k)
    for k in _FROM_FILE - seen:      # removed from pdrn.env since the last read
        os.environ.pop(k, None)
    _FROM_FILE.clear()
    _FROM_FILE.update(seen)
    return True


_load_env_file()


def _env(name, default=""):
    v = os.environ.get("VG_" + name)
    return default if v is None or v.strip() == "" else v.strip()


DB_NAME = _env("PDRN_DB_NAME", "PDRN")


def _log(msg):
    print(f"[pdrn_sync] {datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


# ---------------- feeds ----------------

def _fy_start_year(d=None):
    """SAP fiscal year (April-March, named by its start year): Nov 2026 -> 2026, Feb 2027 -> 2026."""
    d = d or datetime.now()
    return d.year if d.month >= 4 else d.year - 1


def _csv(v):
    return [x.strip() for x in str(v or "").split(",") if x.strip()]


class Feed:
    def __init__(self, key, report, path, table, status_field, allowed, count_where,
                 enabled_default, allow_empty=False):
        self.key, self.report, self.path = key, report, path
        self.env = key.upper()
        self.table, self.log_table = table, table + "_LOAD"
        self.status_field = status_field      # column checked on every row (None = no check)
        self.allowed = allowed                # set of allowed values, or None = any non-blank
        self.count_where = count_where        # (column, value) counted for the ratio check, None = all rows
        self.enabled_default = enabled_default
        self.allow_empty = allow_empty        # a call may return 0 rows (a project with no postings)
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
        k = self.key
        if k == "pdrn":
            s["extra"] = {"bukrs": _env("PDRN_BUKRS", "1010,1070,2010,2070,3070"),
                          "status": _env("PDRN_STATUS", "all")}
        elif k == "invr":
            # ZSD_INVR "Unit" + "In Transit" (a cancelled unit waiting to be released is still stock,
            # e.g. Sky Arc TB-2704) - project totals = both; "Parking" is not a unit
            s["types"] = _env("INVR_TYPE", "units,transit")
            s["extra"] = {}
        elif k == "zrec":
            s["companies"] = _env("ZREC_BUKRS", COMPANIES)
            s["extra"] = {"from": _env("ZREC_FROM", "2015-01-01")}
        elif k == "zdapp":
            s["companies"] = _env("ZDAPP_BUKRS", COMPANIES)
            s["views"] = _env("ZDAPP_VIEWS", "due,undue")   # SAP's own Due / Undue radio buttons
            s["extra"] = {}
        elif k == "zalr_np":
            s["views"] = _env("ZALR_NP_VIEWS", "np_po,np_summary,np_total_actual")
            s["batch"] = int(_env("ZALR_NP_BATCH", "5"))    # definitions per SAP call
            # Non-Project WBS are per fiscal year (…-F26-…): load the CURRENT fiscal year only, as the
            # manual FY exports did (it moves to the next year by itself every April)
            # the manual FY exports = the WBS of that fiscal year ("…-F26-…"), all their postings;
            # a year filter is added only when VG_ZALR_NP_YEARS is set
            s["extra"] = {"wbs": _env("ZALR_NP_WBS", f"*F{_fy_start_year() % 100:02d}*")}
            if _env("ZALR_NP_YEARS"):
                s["extra"]["year"] = _env("ZALR_NP_YEARS")
        elif k == "zalr_pj":
            s["psphi"] = _env("ZALR_PJ_PSPHI", "*")
            s["extra"] = {}
        return s

    def scope(self, s):
        """Text for the load log: which calls this run makes."""
        if self.key == "zrec":
            return (f"companies {s['companies']} · per year from {s['extra'].get('from')}, "
                    f"per month for the last two years")
        if self.key == "zdapp":
            return f"companies {s['companies']} · {s['views']}"
        if self.key == "zalr_np":
            return (f"{s['views']} · WBS {s['extra']['wbs']}"
                    + (f" · years {s['extra']['year']}" if "year" in s["extra"] else "")
                    + f" · {len(_np_defs())} definitions")
        if self.key == "zalr_pj":
            return "project definitions " + s["psphi"]
        return s["werks"]

    def calls(self, s):
        """[(label, params)] - one SAP call each, small enough for SAP's ICM timeout."""
        k = self.key
        if k in ("pdrn", "invr"):
            ws = _csv(s["werks"]) if s["werks"] != "*" else [None]
            types = _csv(s["types"]) if k == "invr" else [None]
            return [(" ".join(x for x in ("project", w or "all", t) if x),
                     {**({"werks": w} if w else {}), **({"type": t} if t else {})})
                    for w in ws for t in types]
        if k == "zrec":
            # one call per company per calendar year, and per MONTH for last year and this year (the
            # busy ones - Nature's Court 2026 alone ran past the timeout); small calls stay far below
            # SAP's ICM timeout and a receipt falls in exactly one window, so nothing repeats
            import calendar
            start = int(str(s["extra"].get("from", "2015-01-01"))[:4])
            today = datetime.now().date()
            windows = []
            for y in range(start, today.year + 1):
                if y < today.year - 1:
                    windows.append((str(y), f"{y}-01-01", f"{y}-12-31"))
                    continue
                for m in range(1, 13):
                    if (y, m) > (today.year, today.month):
                        break
                    last = calendar.monthrange(y, m)[1]
                    to = today.isoformat() if (y, m) == (today.year, today.month) else f"{y}-{m:02d}-{last}"
                    windows.append((f"{y}-{m:02d}", f"{y}-{m:02d}-01", to))
            return [(f"company {b} {w}", {"bukrs": b, "from": fr, "to": to})
                    for b in _csv(s["companies"]) for w, fr, to in windows]
        if k == "zdapp":
            return [(f"company {b} {v}", {"bukrs": b, "view": v})
                    for b in _csv(s["companies"]) for v in _csv(s["views"])]
        if k == "zalr_np":
            # every view, a few Non-Project definitions per call (one call for all of them ran past
            # SAP's ICM timeout); each WBS belongs to one definition, so nothing repeats
            defs, n = _np_defs(), max(1, s["batch"])
            groups = [defs[i:i + n] for i in range(0, len(defs), n)]
            return [(f"view {v} {g[0]}-{g[-1]}", {"view": v, "psphi": ",".join(g)})
                    for v in _csv(s["views"]) for g in groups]
        if k == "zalr_pj":
            if s["psphi"] != "*":
                defs = _csv(s["psphi"])
            else:
                # every Project definition SAP knows (budget profile ZPSWB2 = Non-Project, skipped)
                rows, _ = _fetch(self, s, {"view": "projlist", "psphi": "*"})
                np_defs = set(_np_defs())
                defs = sorted(({str(r.get("PSPID") or "").strip().replace("/", "").upper() for r in rows
                                if str(r.get("BPROF") or "").strip() != "ZPSWB2"}
                               | set(_pj_extra())) - {""} - np_defs)
                if not defs:
                    raise RuntimeError("view=projlist returned no Project definitions")
            return [(f"project {d}", {"view": "project", "psphi": d}) for d in defs]
        raise RuntimeError(f"no call plan for feed {k}")

    def counted(self, rows):
        if self.count_where is None:
            return len(rows)
        col, val = self.count_where
        return sum(1 for r in rows if str(r.get(col) or "").strip() == val)


COMPANIES = "1010,1070,2010,2070,3070"
# Non-Project definitions (from the 30-Oct-2026 ZALR Non-Project exports). In PRD some of them (HO1000,
# HO2080) do not carry budget profile ZPSWB2, so the profile alone cannot tell them apart: this list
# decides what the Non-Project feed loads and what the Project feed leaves out. New ones: add them to
# VG_ZALR_NP_PSPHI in pdrn.env (the full comma list replaces this one).
NP_DEFS = ("HO1000,HO2080,RE0062,RE0075,RE1040,RE1050,RE1060,RE1080,RE1090,RE1110,RE1130,RE2000,RE2020,"
           "RE2030,RE2040,RE2050,RE2060,RE2090,RE3000,RE3010,RE3020,RE3030,RE3040,RE3050,RE3060,RE3080,"
           "RE3090,RE4000,RE4010,RE4020,RE4030,RE4050,RE4060,RE4070,RE4080,RE4090,RE5000,RE5010,RE5020,"
           "RE5030,RE5040,RE6600,RE6700,RE6900,RE9800,REM110")


def _pj_extra():
    """Project definitions that carry the Non-Project budget profile in PRD but ARE projects
    (they were in the manual Project export) - always loaded by the Project feed."""
    return [d.replace("/", "").upper() for d in _csv(_env("ZALR_PJ_EXTRA", "RE0111"))]


def _np_defs():
    return [d.replace("/", "").upper() for d in _csv(_env("ZALR_NP_PSPHI", NP_DEFS))]
FEEDS = {
    "invr": Feed("invr", "ZSD_INVR", "/sap/zinvr", "SAP_INVR", "VMSTB", None, None, False),
    "pdrn": Feed("pdrn", "ZSD_PDRN", "/sap/zpdrn", "SAP_PDRN", "BUKST", {"ACTIVE", "CANCELLED"},
                 ("BUKST", "ACTIVE"), True),
    # receipts (Collections, EOI) - one call per company, all receipts since 2015
    "zrec": Feed("zrec", "ZREC_PRINT", "/sap/zrec", "SAP_ZREC", None, None, None, True,
                 allow_empty=True),
    # due / undue as per payment plan - one call per company
    "zdapp": Feed("zdapp", "ZSD_DAPP", "/sap/zdapp", "SAP_ZDAPP", None, None, None, True,
                  allow_empty=True),
    # Budget vs Actual, Non-Project: Item Wise PO + Summary + Total Actual, FY 2024 -> now
    "zalr_np": Feed("zalr_np", "ZALR", "/sap/zalr", "SAP_ZALR_NP", None, None, None, True,
                    allow_empty=True),
    # Budget vs Actual, Project: every Project definition, project to date
    "zalr_pj": Feed("zalr_pj", "ZALR", "/sap/zalr", "SAP_ZALR_PJ", None, None, None, True,
                    allow_empty=True),
}
ORDER = ["invr", "pdrn", "zrec", "zdapp", "zalr_np", "zalr_pj"]   # INVR + PDRN first (same moment)
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

def _fetch(f, s, call):
    """One service call (call = its own parameters). Returns (rows with UPPER-case keys, seconds)."""
    params = dict(s["extra"], saml2="disabled")
    params.update(call)
    where = ", ".join(f"{k}={v}" for k, v in call.items()) or "all"
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
        raise RuntimeError(f"{where}: answer has no rows")
    cnt = ans.get("count", ans.get("COUNT"))
    if cnt is not None and int(cnt) != len(rows):
        raise RuntimeError(f"{where}: count {cnt} but {len(rows)} rows arrived")
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
        if not check_only:
            ensure_schema(f)
            load_id = _log_start(f, trigger, f.scope(s))
        st["progress"] = "planning"
        plan = f.calls(s)
        parts = []
        for i, (label, call) in enumerate(plan, 1):
            st["progress"] = f"{label} ({i}/{len(plan)})"
            rows, secs = _fetch(f, s, call)
            if not rows and not (f.allow_empty or call.get("type") == "transit"):   # most have none
                raise RuntimeError(f"{label}: SAP returned 0 rows - not loading")
            for r in rows:              # which call a row came from (company / view / project)
                r["CALL_KEY"] = label.split(" ", 1)[-1]
            parts.append(f"{label.split(' ', 1)[-1]}:{len(rows)} ({secs:.0f}s)")
            _log(f"{key} {label}: {len(rows)} rows in {secs:.0f}s")
            rows_all.extend(rows)
        if not rows_all:
            raise RuntimeError("SAP returned 0 rows in total - not loading")
        vals = ({str(r.get(f.status_field) or "").strip() for r in rows_all}
                if f.status_field else set())
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
                   table=f"{DB_NAME}.dbo.{f.table}", times=s["times"], projects=f.scope(s),
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
