"""
SAP OData -> SQL sync (scheduler).

Pulls entity sets from the SAP Gateway service ZMM_PURCHASE_DASHBOARD_SRV
on PS4 with a rolling date-range $filter, and upserts the rows into
tables in the local PR2PO database - same dynamic-column upsert design
as nfa_tat_writer (auto-created NVARCHAR(MAX) columns, insert new keys,
update changed rows, skip identical).

Default entity: PO_DATASet keyed on (Ebeln, Ebelp), date field Badat ->
table PR2PO.dbo.ODATA_PO. When the ABAPer adds a PR entity, one env var
adds it - no code change:

    VG_ODATA_ENTITIES = "PO_DATASet|ODATA_PO|Ebeln,Ebelp|Badat;PR_DATASet|ODATA_PR|Banfn,Bnfpo|Erdat"
                         entity  | table  | key cols  | date field

Config (env, VG_ prefix):
    VG_ODATA_BASE      service root, default
                       https://vhsmwps4ci.sap.smartworlddevelopers.com:20400/sap/opu/odata/sap/ZMM_PURCHASE_DASHBOARD_SRV
    VG_ODATA_USER / VG_ODATA_PASSWORD   SAP user (basic auth) - REQUIRED
    VG_ODATA_VERIFY    "1" to verify TLS (default off: SAP self-signed cert)
    VG_ODATA_ROLLING_DAYS      window pulled each cycle (default 10)
    VG_ODATA_INTERVAL_SECONDS  cycle interval (default 300)
    VG_ODATA_BACKFILL_START    one-time full backfill start date, pulled
                               month by month on first cycle after start
                               (e.g. "2026-06-01"); "" disables.

    VG_ODATA_FULL_EVERY        full re-scan (BACKFILL_START -> today) every
                               N cycles (default 12 = hourly at 300 s);
                               other cycles pull only the rolling window.

Since the old SAP BI database (SWDBIDB on .33) was erased, this OData feed
is the ONLY live SAP source for PR2PO. The PR2PO database is created on
this server (.28) automatically; old SAP_PR / SAP_PO mirror rows stay as
read-only history.

Standalone:  python odata_sync.py --probe    (test login, list entity sets
                                              + key/date fields, suggest
                                              VG_ODATA_ENTITIES)
             python odata_sync.py --once     (create DB + one full scan)
             python odata_sync.py            (runs the scheduler)
"""

import os
import re
import sys
import time
import threading
from datetime import datetime, date, timedelta

import requests
import pyodbc
import urllib3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db_config as cfg

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_lock = threading.Lock()

# heartbeat read by /pr2po (meta.sync.loop) and /pr2po/health
STATUS = {"running": False, "configured": False, "last_run": None, "last_ok": None,
          "last_full_ok": None, "last_error": None, "last_error_at": None, "results": None}


def _env(name, default):
    return os.environ.get("VG_" + name, default)


ODATA_BASE = _env(
    "ODATA_BASE",
    "https://vhsmwps4ci.sap.smartworlddevelopers.com:20400"
    "/sap/opu/odata/sap/ZMM_PURCHASE_DASHBOARD_SRV",
).rstrip("/")
ODATA_USER = _env("ODATA_USER", "")
ODATA_PASSWORD = _env("ODATA_PASSWORD", "")
ODATA_VERIFY = _env("ODATA_VERIFY", "0") == "1"
ROLLING_DAYS = int(_env("ODATA_ROLLING_DAYS", "10"))
# SAP logon client (e.g. "300"); empty = the Gateway's default client
ODATA_CLIENT = _env("ODATA_CLIENT", "").strip()
FULL_EVERY = max(1, int(_env("ODATA_FULL_EVERY", "12")))
INTERVAL = int(_env("ODATA_INTERVAL_SECONDS", "300"))
BACKFILL_START = _env("ODATA_BACKFILL_START", "2026-06-01")
PR2PO_DB_NAME = _env("PR2PO_DB_NAME", "PR2PO")

# entity | table | key columns | date field   (; separated)
ENTITIES_SPEC = _env("ODATA_ENTITIES", "PO_DATASet|ODATA_PO|Ebeln,Ebelp|Badat")


def _entities():
    out = []
    for part in ENTITIES_SPEC.split(";"):
        part = part.strip()
        if not part:
            continue
        entity, table, keys, datefield = [p.strip() for p in part.split("|")]
        out.append({"entity": entity, "table": table,
                    "keys": [k.strip() for k in keys.split(",")],
                    "datefield": datefield})
    return out


# ---------------- SQL helpers (nfa_tat_writer pattern) ----------------

def _local_connection_string(database=None):
    return (
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
        f"DATABASE={database or PR2PO_DB_NAME};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
    )


def _sanitize(name):
    n = re.sub(r"[^0-9A-Za-z_]", "_", str(name)).strip("_") or "col"
    return ("c_" + n if n[0].isdigit() else n)[:120]


def ensure_database():
    conn = pyodbc.connect(_local_connection_string("master"), autocommit=True)
    try:
        conn.cursor().execute(
            "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = ?) "
            "EXEC('CREATE DATABASE [' + ? + ']')", PR2PO_DB_NAME, PR2PO_DB_NAME)
    finally:
        conn.close()


def ensure_table(conn, table, keys):
    cur = conn.cursor()
    key_defs = ", ".join(f"[{k}] NVARCHAR(100) NOT NULL" for k in keys)
    pk = ", ".join(f"[{k}]" for k in keys)
    cur.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = ?) "
        f"EXEC('CREATE TABLE [dbo].[{table}] ({key_defs}, "
        "fetched_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(), "
        "first_seen DATETIME2 NOT NULL DEFAULT SYSDATETIME(), "
        f"CONSTRAINT [PK_{table}] PRIMARY KEY ({pk}))')", table)
    conn.commit()
    cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table)
    known = {r[0] for r in cur.fetchall()}
    # presence-tracking metadata (typed): last time SAP returned the row,
    # and when it first went missing from the feed (deleted-in-SAP signal,
    # since the entity silently drops deleted items instead of Loekz='X').
    for meta in ("last_seen", "missing_since"):
        if meta not in known:
            cur.execute(f"ALTER TABLE [dbo].[{table}] ADD [{meta}] DATETIME2 NULL")
            conn.commit()
            known.add(meta)
    return known


def ensure_columns(conn, table, known, wanted):
    missing = [c for c in wanted if c not in known]
    if missing:
        cur = conn.cursor()
        for c in missing:
            cur.execute(f"ALTER TABLE [dbo].[{table}] ADD [{c}] NVARCHAR(MAX) NULL")
        conn.commit()
        known.update(missing)


# ---------------- OData fetch ----------------

_MS_DATE = re.compile(r"^/Date\((-?\d+)(?:[+-]\d+)?\)/$")


def _norm_value(v):
    """JSON value -> string for storage. OData v2 dates '/Date(ms)/' -> ISO."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (dict, list)):
        return None  # skip deferred/nav properties
    s = str(v).strip()
    m = _MS_DATE.match(s)
    if m:
        try:
            return datetime.utcfromtimestamp(int(m.group(1)) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OSError, OverflowError):
            return s
    return s


def fetch_window(entity, datefield, start_d, end_d):
    """All rows of `entity` with datefield in [start_d, end_d] (dates)."""
    filt = (f"{datefield} ge datetime'{start_d.isoformat()}T00:00:00' and "
            f"{datefield} le datetime'{end_d.isoformat()}T23:59:59'")
    url = f"{ODATA_BASE}/{entity}"
    rows, skip = [], 0
    while True:
        params = {"$filter": filt, "$format": "json", **_client()}
        if skip:
            params["$skip"] = str(skip)
        resp = requests.get(url, params=params,
                            auth=(ODATA_USER, ODATA_PASSWORD),
                            verify=ODATA_VERIFY, timeout=120,
                            headers={"Accept": "application/json"})
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("d", {}).get("results", data.get("d", []) if isinstance(data.get("d"), list) else [])
        if not isinstance(batch, list):
            batch = []
        rows.extend(batch)
        # follow server paging if present, else stop when batch < server cap
        if data.get("d", {}).get("__next") if isinstance(data.get("d"), dict) else None:
            skip += len(batch)
            if not batch:
                break
        else:
            break
    return rows


# ---------------- upsert ----------------

META_COLS = {"fetched_at", "first_seen", "last_seen", "missing_since"}


def upsert(table, keys, raw_rows, mark_missing=False, missing_scope=None):
    """Insert/update `raw_rows`. Every returned row gets last_seen=now and
    missing_since=NULL. With mark_missing=True (only when raw_rows covers
    the FULL synced date range), rows in the table that SAP did not return
    get missing_since=now - the deleted-in-SAP signal. missing_scope is an
    optional (datefield, start_iso) tuple limiting the marking to rows the
    scan actually covered. Returns (ins, upd, same, total, newly_missing)."""
    sanitized = []
    all_cols = set()
    for r in raw_rows:
        row = {}
        for k, v in r.items():
            if k == "__metadata":
                continue
            val = _norm_value(v)
            if isinstance(v, (dict, list)):
                continue
            row[_sanitize(k)] = val
        sanitized.append(row)
        all_cols.update(row.keys())
    # one row per key tuple (last wins)
    dedup = {}
    for row in sanitized:
        key = tuple(str(row.get(k) or "") for k in keys)
        if all(key):
            dedup[key] = row
    rows = list(dedup.values())
    data_cols = sorted(all_cols - set(keys) - META_COLS)

    with _lock:
        conn = pyodbc.connect(_local_connection_string(), autocommit=False)
        try:
            known = ensure_table(conn, table, keys)
            ensure_columns(conn, table, known, sorted(all_cols - META_COLS))
            cur = conn.cursor()
            key_where = " AND ".join(f"[{k}] = ?" for k in keys)
            col_list = ", ".join(f"[{c}]" for c in (keys + data_cols))
            cur.execute(f"SELECT {col_list} FROM [dbo].[{table}]")
            existing = {}
            for rec in cur.fetchall():
                kt = tuple(str(rec[i] or "") for i in range(len(keys)))
                existing[kt] = {c: (None if rec[len(keys) + i] is None else str(rec[len(keys) + i]))
                                for i, c in enumerate(data_cols)}
            now = datetime.now()
            ins = upd = same = 0
            for key, row in dedup.items():
                new_vals = {c: (None if row.get(c) is None else str(row.get(c))) for c in data_cols}
                if key not in existing:
                    cols = list(keys) + ["fetched_at", "first_seen", "last_seen"] + data_cols
                    ph = ", ".join(["?"] * len(cols))
                    cur.execute(
                        f"INSERT INTO [dbo].[{table}] ({', '.join(f'[{c}]' for c in cols)}) VALUES ({ph})",
                        list(key) + [now, now, now] + [new_vals[c] for c in data_cols])
                    ins += 1
                elif existing[key] != new_vals:
                    set_list = ", ".join(f"[{c}] = ?" for c in data_cols)
                    cur.execute(
                        f"UPDATE [dbo].[{table}] SET {set_list}, fetched_at = ?, "
                        f"last_seen = ?, missing_since = NULL WHERE {key_where}",
                        [new_vals[c] for c in data_cols] + [now, now] + list(key))
                    upd += 1
                else:
                    cur.execute(
                        f"UPDATE [dbo].[{table}] SET last_seen = ?, missing_since = NULL "
                        f"WHERE {key_where}", [now] + list(key))
                    same += 1
            newly_missing = 0
            if mark_missing and dedup:
                # rows SAP no longer returns -> deleted in SAP (inferred)
                cur.execute("CREATE TABLE #seen (" +
                            ", ".join(f"[{k}] NVARCHAR(100) NOT NULL" for k in keys) + ")")
                seen = list(dedup.keys())
                ph_row = "(" + ", ".join(["?"] * len(keys)) + ")"
                for i in range(0, len(seen), 500):
                    chunk = seen[i:i + 500]
                    cur.execute(
                        f"INSERT INTO #seen VALUES {', '.join([ph_row] * len(chunk))}",
                        [v for key in chunk for v in key])
                join = " AND ".join(f"s.[{k}] = t.[{k}]" for k in keys)
                scope_sql, scope_args = "", []
                if missing_scope and missing_scope[0] in known:
                    scope_sql = f" AND t.[{missing_scope[0]}] >= ?"
                    scope_args = [missing_scope[1]]
                cur.execute(
                    f"UPDATE t SET missing_since = ? FROM [dbo].[{table}] t "
                    f"WHERE t.missing_since IS NULL{scope_sql} AND NOT EXISTS "
                    f"(SELECT 1 FROM #seen s WHERE {join})",
                    [now] + scope_args)
                newly_missing = cur.rowcount
                cur.execute("DROP TABLE #seen")
            conn.commit()
            return ins, upd, same, len(rows), newly_missing
        finally:
            conn.close()


# ---------------- scheduler ----------------

_backfill_done = False


def _month_windows(start_d, end_d):
    cur = date(start_d.year, start_d.month, 1)
    while cur <= end_d:
        nxt = date(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)
        yield max(cur, start_d), min(nxt - timedelta(days=1), end_d)
        cur = nxt


def sync_once(backfill=False, full=True):
    """One cycle. A FULL cycle re-scans the whole range from BACKFILL_START
    (month windows): that refreshes changes to old documents (late releases,
    cancellations) and lets us detect rows SAP stopped returning = deleted
    in SAP (the entity silently drops deleted items instead of flagging
    Loekz). Other cycles pull only the rolling window (fast, every 5 min)."""
    results = {}
    today = date.today()
    for ent in _entities():
        windows, scan_start = [], None
        if BACKFILL_START and (full or backfill):
            try:
                scan_start = date.fromisoformat(BACKFILL_START)
                windows = list(_month_windows(scan_start, today))
            except ValueError:
                windows = []
        if not windows:
            scan_start = today - timedelta(days=ROLLING_DAYS)
            windows = [(scan_start, today)]
        rows = []
        for w0, w1 in windows:
            rows.extend(fetch_window(ent["entity"], ent["datefield"], w0, w1))
        # mark_missing only when the fetch worked (rows came back) - a SAP
        # hiccup returning nothing must not flag the whole table deleted
        results[ent["table"]] = upsert(
            ent["table"], ent["keys"], rows,
            mark_missing=bool(rows),
            missing_scope=(_sanitize(ent["datefield"]), scan_start.isoformat()))
    return results


def run_forever(interval_seconds=None):
    global _backfill_done
    interval = interval_seconds or INTERVAL
    STATUS["running"] = True
    if not ODATA_USER:
        STATUS["last_error"] = "VG_ODATA_USER / VG_ODATA_PASSWORD not set on the API server"
        STATUS["last_error_at"] = datetime.now()
        print("[odata_sync] VG_ODATA_USER not set - scheduler idle until configured.")
        return
    STATUS["configured"] = True
    try:
        ensure_database()
    except Exception as e:  # noqa: BLE001
        STATUS["last_error"], STATUS["last_error_at"] = f"create PR2PO database: {e}"[:500], datetime.now()
        print(f"[odata_sync] cannot create/open {PR2PO_DB_NAME}: {e}")
    print(f"[odata_sync] {ODATA_BASE} -> {cfg.DB_SERVER}/{PR2PO_DB_NAME} "
          f"every {interval}s as '{ODATA_USER}' (verify={ODATA_VERIFY}, "
          f"rolling {ROLLING_DAYS}d, full scan from {BACKFILL_START or 'off'} every {FULL_EVERY} cycles)")
    cycle = 0
    while True:
        STATUS["last_run"] = datetime.now()
        full = (not _backfill_done) or cycle % FULL_EVERY == 0
        try:
            ensure_database()
            results = sync_once(backfill=not _backfill_done, full=full)
            _backfill_done = True
            STATUS["last_ok"] = datetime.now()
            if full:
                STATUS["last_full_ok"] = STATUS["last_ok"]
            STATUS["results"] = {t: {"new": r[0], "updated": r[1], "unchanged": r[2],
                                     "source_rows": r[3], "gone_from_sap": r[4],
                                     "full_scan": full} for t, r in results.items()}
            changed = {t: r for t, r in results.items() if r[0] or r[1] or r[4]}
            if changed:
                msg = " · ".join(
                    f"{t}: +{r[0]} new, ~{r[1]} updated of {r[3]}"
                    + (f", {r[4]} gone-from-SAP" if r[4] else "")
                    for t, r in changed.items())
                print(f"[odata_sync] {datetime.now().strftime('%H:%M:%S')} {msg}")
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            hint = {401: " - SAP rejected the login (password / locked user / client)",
                    403: " - SAP user lacks S_SERVICE authorization for the service"}.get(code, "")
            STATUS["last_error"], STATUS["last_error_at"] = f"HTTP {code} from SAP{hint}", datetime.now()
            print(f"[odata_sync] HTTP {code} from SAP{hint}")
        except Exception as e:  # noqa: BLE001
            STATUS["last_error"], STATUS["last_error_at"] = str(e)[:500], datetime.now()
            print(f"[odata_sync] ERROR: {e}")
        cycle += 1
        time.sleep(interval)


def _client():
    return {"sap-client": ODATA_CLIENT} if ODATA_CLIENT else {}


def ping(timeout=10):
    """Quick reachability/auth check of the service root (for /pr2po/health)."""
    if not ODATA_USER:
        return "not configured (VG_ODATA_USER / VG_ODATA_PASSWORD missing)"
    try:
        r = requests.get(f"{ODATA_BASE}/", params={"$format": "json", **_client()},
                         auth=(ODATA_USER, ODATA_PASSWORD), verify=ODATA_VERIFY,
                         timeout=timeout, headers={"Accept": "application/json"})
        if r.status_code == 401:
            return ("error: HTTP 401 - SAP rejected the LOGIN: wrong user/password, user locked, "
                    "or user not in the default client (set VG_ODATA_CLIENT)")
        if r.status_code == 403:
            return "error: HTTP 403 - login OK but user lacks S_SERVICE authorization for this service"
        r.raise_for_status()
        return "ok"
    except Exception as e:  # noqa: BLE001
        return f"error: {e}"


def probe():
    """Print what the service exposes and the VG_ODATA_ENTITIES line to set.
    Never prints credentials."""
    import xml.etree.ElementTree as ET
    print(f"service : {ODATA_BASE}")
    print(f"user    : {ODATA_USER or '(VG_ODATA_USER not set)'}  client: {ODATA_CLIENT or '(default)'}")
    print(f"root    : {ping()}")
    r = requests.get(f"{ODATA_BASE}/$metadata", params=_client(), auth=(ODATA_USER, ODATA_PASSWORD),
                     verify=ODATA_VERIFY, timeout=60)
    print(f"metadata: HTTP {r.status_code}")
    if r.status_code != 200:
        return
    root = ET.fromstring(r.content)
    local = lambda t: t.rsplit("}", 1)[-1]  # noqa: E731
    types = {}
    for el in root.iter():
        if local(el.tag) == "EntityType":
            keys = [k.get("Name") for k in el.iter() if local(k.tag) == "PropertyRef"]
            props = [(p.get("Name"), p.get("Type")) for p in el if local(p.tag) == "Property"]
            types[el.get("Name")] = (keys, props)
    specs = []
    for el in root.iter():
        if local(el.tag) != "EntitySet":
            continue
        name, etype = el.get("Name"), el.get("EntityType", "").split(".")[-1]
        keys, props = types.get(etype, ([], []))
        dates = [p for p, t in props if t and "DateTime" in t]
        datef = next((d for d in ("Badat", "Erdat", "Aedat", "Bedat") if d in dates), dates[0] if dates else "?")
        print(f"\n{name}  (type {etype})\n  keys : {', '.join(keys)}\n  dates: {', '.join(dates) or '-'}"
              f"\n  {len(props)} fields: {', '.join(p for p, _ in props)}")
        table = "ODATA_PR" if "PR" in name.upper().replace("PROC", "") and "PO" not in name.upper() else \
                "ODATA_PO" if "PO" in name.upper() else "ODATA_" + re.sub(r"\W", "_", name.upper())[:30]
        specs.append(f"{name}|{table}|{','.join(keys)}|{datef}")
    print("\nSuggested (set as machine env var, then restart VendorGlobeAPI):")
    print(f'  VG_ODATA_ENTITIES = "{";".join(specs)}"')
    print(f"Currently set:\n  VG_ODATA_ENTITIES = \"{ENTITIES_SPEC}\"")


def start_background_thread():
    t = threading.Thread(target=run_forever, daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    if "--probe" in sys.argv:
        probe()
    elif "--once" in sys.argv:
        ensure_database()
        for t, r in sync_once(backfill=True, full=True).items():
            print(f"{t}: +{r[0]} new, ~{r[1]} updated, {r[2]} unchanged, "
                  f"{r[3]} rows from SAP, {r[4]} gone-from-SAP")
    else:
        run_forever()
