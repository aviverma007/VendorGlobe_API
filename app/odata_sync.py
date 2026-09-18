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

Standalone:  python odata_sync.py            (runs the scheduler)
             python odata_sync.py --once     (single cycle, then exit)
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
    return {r[0] for r in cur.fetchall()}


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
        params = {"$filter": filt, "$format": "json"}
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

def upsert(table, keys, raw_rows):
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
    data_cols = sorted(all_cols - set(keys) - {"fetched_at", "first_seen"})

    with _lock:
        conn = pyodbc.connect(_local_connection_string(), autocommit=False)
        try:
            known = ensure_table(conn, table, keys)
            ensure_columns(conn, table, known, sorted(all_cols))
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
                    cols = list(keys) + ["fetched_at", "first_seen"] + data_cols
                    ph = ", ".join(["?"] * len(cols))
                    cur.execute(
                        f"INSERT INTO [dbo].[{table}] ({', '.join(f'[{c}]' for c in cols)}) VALUES ({ph})",
                        list(key) + [now, now] + [new_vals[c] for c in data_cols])
                    ins += 1
                elif existing[key] != new_vals:
                    set_list = ", ".join(f"[{c}] = ?" for c in data_cols)
                    cur.execute(
                        f"UPDATE [dbo].[{table}] SET {set_list}, fetched_at = ? WHERE {key_where}",
                        [new_vals[c] for c in data_cols] + [now] + list(key))
                    upd += 1
                else:
                    same += 1
            conn.commit()
            return ins, upd, same, len(rows)
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


def sync_once(backfill=False):
    results = {}
    today = date.today()
    for ent in _entities():
        windows = []
        if backfill and BACKFILL_START:
            try:
                bstart = date.fromisoformat(BACKFILL_START)
                windows = list(_month_windows(bstart, today))
            except ValueError:
                windows = []
        if not windows:
            windows = [(today - timedelta(days=ROLLING_DAYS), today)]
        tot = [0, 0, 0, 0]
        for w0, w1 in windows:
            rows = fetch_window(ent["entity"], ent["datefield"], w0, w1)
            r = upsert(ent["table"], ent["keys"], rows)
            tot = [a + b for a, b in zip(tot, r)]
        results[ent["table"]] = tuple(tot)
    return results


def run_forever(interval_seconds=None):
    global _backfill_done
    interval = interval_seconds or INTERVAL
    if not ODATA_USER:
        print("[odata_sync] VG_ODATA_USER not set - scheduler idle until configured.")
        return
    ensure_database()
    print(f"[odata_sync] {ODATA_BASE} -> {cfg.DB_SERVER}/{PR2PO_DB_NAME} "
          f"every {interval}s as '{ODATA_USER}' (verify={ODATA_VERIFY}, "
          f"rolling {ROLLING_DAYS}d, backfill from {BACKFILL_START or 'off'})")
    while True:
        try:
            results = sync_once(backfill=not _backfill_done)
            _backfill_done = True
            changed = {t: r for t, r in results.items() if r[0] or r[1]}
            if changed:
                msg = " · ".join(f"{t}: +{r[0]} new, ~{r[1]} updated of {r[3]}"
                                 for t, r in changed.items())
                print(f"[odata_sync] {datetime.now().strftime('%H:%M:%S')} {msg}")
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            hint = (" - SAP user lacks S_SERVICE authorization for the service"
                    if code in (401, 403) else "")
            print(f"[odata_sync] HTTP {code} from SAP{hint}")
        except Exception as e:  # noqa: BLE001
            print(f"[odata_sync] ERROR: {e}")
        time.sleep(interval)


def start_background_thread():
    t = threading.Thread(target=run_forever, daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    if "--once" in sys.argv:
        ensure_database()
        print(sync_once(backfill=True))
    else:
        run_forever()
