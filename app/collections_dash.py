"""Collection dashboard data — read the CRM team's three Excel files
straight from the shared folder, no database needed.

Folder (override with VG_COLLECTION_DIR):
    \\\\WIN-PJQA0USC6HT\\Users\\anirudh.verma\\Downloads\\CRM\\CRM DATA REPORTS\\CRM\\COLLECTION
Files (fixed names; falls back to the newest file matching the prefix,
so dated names also work):
    Collection Master.xlsx   -> "Master." sheet (per-unit ledger: TCV,
                                Demanded, Recd., Net Due — the team's own
                                numbers) + "Inventory" pivot (Done/Pending
                                allotment counts with money, in Cr)
    Daily Collection Report.xlsx -> one receipts sheet per project +
                                'Fina l-2 ' RM/project monthly targets

The endpoint caches each parsed file keyed by its modified-time: the
team saves the Excel, the next dashboard load re-reads only what
changed. Files are copied to a temp path before opening so a workbook
someone has open in Excel never breaks the API. Excel error strings
(#N/A, #REF!, ...) and the pre-filled empty rows are dropped.
"""
import os
import glob
import shutil
import tempfile
import datetime
import threading

import openpyxl
from flask import jsonify, request

COLLECTION_DIR = os.environ.get(
    "VG_COLLECTION_DIR",
    r"\\WIN-PJQA0USC6HT\Users\anirudh.verma\Downloads\CRM\CRM DATA REPORTS\CRM\COLLECTION",
)
FILES = {
    "master": ("Collection Master.xlsx", "Collection Master*"),
    "daily": ("Daily Collection Report.xlsx", "Daily Collection*"),
}
# Daily workbook: sheets that are pivots/rosters, not receipt logs
DAILY_SKIP = {"summary", "rm sheet", "final", "fina l-2", "aug - aop sheet", "m3m",
              "september roaster"}

_lock = threading.Lock()
_cache = {}          # key -> (mtime, parsed)

ERRS = {"#N/A", "#REF!", "#DIV/0!", "#VALUE!", "#NAME?", "0", "", "None"}


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return None if s in ("", "#N/A", "#REF!", "#DIV/0!", "#VALUE!", "#NAME?", "None") else s


def _num(v):
    try:
        f = float(v)
        return round(f, 2)
    except (TypeError, ValueError):
        return 0.0


def _iso(v):
    if isinstance(v, datetime.datetime):
        return v.date().isoformat()
    if isinstance(v, datetime.date):
        return v.isoformat()
    s = _clean(v)
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.datetime.strptime(s[:10], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _norm_proj(p):
    s = (_clean(p) or "").upper()
    s = " ".join(s.split())
    s = s.replace(" - ", "-").replace(" -", "-").replace("- ", "-")
    if s.startswith("ONE "):        # Daily sheets say "One DXP-2", targets say "DXP-2"
        s = s[4:]
    return s


def _find(kind):
    fixed, pattern = FILES[kind]
    path = os.path.join(COLLECTION_DIR, fixed)
    if os.path.exists(path):
        return path
    hits = sorted(glob.glob(os.path.join(COLLECTION_DIR, pattern + ".xlsx")),
                  key=os.path.getmtime, reverse=True)
    return hits[0] if hits else None


def _open_copy(path):
    """Copy to temp first so an Excel lock never breaks the read."""
    fd, tmp = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    shutil.copyfile(path, tmp)
    try:
        return openpyxl.load_workbook(tmp, read_only=True, data_only=True), tmp
    except Exception:
        os.unlink(tmp)
        raise


def _rows_ledger(ws):
        rows = ws.iter_rows(values_only=True)
        next(rows)  # header
        out = []
        for r in rows:
            if len(r) < 46 or not _clean(r[3]):     # Reg.No required
                continue
            out.append({
                "proj": _norm_proj(r[1]), "phase": _clean(r[2]), "reg": _clean(r[3]),
                "unit": _clean(r[4]), "allot": _clean(r[5]), "name": _clean(r[6]),
                "profile": _clean(r[7]), "allotDate": _iso(r[8]), "unitType": _clean(r[9]),
                "plan": _clean(r[10]), "planType": _clean(r[11]), "broker": _clean(r[12]),
                "area": _num(r[15]), "rate": _num(r[17]),
                "tcv": _num(r[18]), "dem": _num(r[19]), "rec": _num(r[20]), "due": _num(r[21]),
                "recPct": _num(r[24]),
                "letter": _clean(r[25]), "letterDate": _iso(r[26]), "letterDue": _iso(r[27]),
                "rm": _clean(r[28]) or _clean(r[29]),
                "rmFinal": _clean(r[29]) or _clean(r[28]),
                "remarks": (_clean(r[30]) or "")[:220] or None,
                "rmStatus": _clean(r[31]),
                "statusV": _clean(r[32]) if len(r) > 32 else None,
                "benefit": _num(r[22]),
                "funding": _clean(r[34]), "bank": _clean(r[35]),
                "sanctDate": _iso(r[36]), "sanctAmt": _num(r[38]),
                "bba": _clean(r[39]), "bbaDate": _iso(r[40]),
                "possession": _clean(r[47]) if len(r) > 47 else None,
            })
        return out


def _parse_targets(wb):
    """RM x project targets from 'Fina l-2 ' (TGT/RECD/BLNC triplets, in Cr);
    falls back to 'Final' (targets only)."""
    name3 = next((n for n in wb.sheetnames if n.strip().lower().startswith("fina l-2")), None)
    out = []
    if name3:
        rows = list(wb[name3].iter_rows(values_only=True))
        if len(rows) > 3:
            projs = rows[1]
            cols = [(i, _norm_proj(projs[i])) for i in range(3, len(projs)) if _clean(projs[i])]
            for r in rows[3:]:
                rm = _clean(r[2]) if len(r) > 2 else None
                if not rm or rm.lower() in ("total", "rm name"):
                    continue
                for ci, pj in cols:
                    if pj.startswith("GRAND") or "ACHIEV" in pj or "%" in pj:
                        continue
                    tgt = _num(r[ci]) if len(r) > ci else 0.0
                    recd = _num(r[ci + 1]) if len(r) > ci + 1 else 0.0
                    if tgt == 0 and recd == 0:
                        continue
                    out.append({"rm": rm, "proj": pj, "tgt": round(tgt, 4), "recd": round(recd, 4)})
        if out:
            return out
    if "Final" in wb.sheetnames:
        rows = list(wb["Final"].iter_rows(values_only=True))
        if len(rows) > 3:
            projs = rows[2]
            for r in rows[3:]:
                rm = _clean(r[2]) if len(r) > 2 else None
                if not rm or rm.lower() in ("total",):
                    continue
                for i in range(3, len(projs)):
                    pj = _norm_proj(projs[i])
                    if not pj or pj.startswith("GRAND"):
                        continue
                    tgt = _num(r[i]) if len(r) > i else 0.0
                    if tgt:
                        out.append({"rm": rm, "proj": pj, "tgt": round(tgt, 4), "recd": 0.0})
    return out


def _parse_daily(path):
    wb, tmp = _open_copy(path)
    try:
        out = []
        for ws in wb.worksheets:
            if ws.title.strip().lower() in DAILY_SKIP:
                continue
            rows = ws.iter_rows(values_only=True)
            try:
                next(rows)  # header
            except StopIteration:
                continue
            proj = _norm_proj(ws.title)
            for r in rows:
                if len(r) < 13:
                    continue
                amt = _num(r[4])
                if amt == 0 and not _iso(r[8]):
                    continue                      # pre-filled empty row
                out.append({
                    "proj": proj, "reg": _clean(r[1]), "name": _clean(r[2]),
                    "unit": _clean(r[3]), "amt": amt, "mode": _clean(r[5]),
                    "chq": _clean(r[6]), "bank": _clean(r[7]),
                    "rcptDate": _iso(r[8]) or _iso(r[9]) or _iso(r[11]), "chqDate": _iso(r[9]),
                    "clearDate": _iso(r[10]), "created": _clean(r[11]),
                    "rm": _clean(r[12]),
                    "milestone": _clean(r[15]) if len(r) > 15 else None,
                    "dueDate": _iso(r[16]) if len(r) > 16 else None,
                })
        return {"receipts": out, "targets": _parse_targets(wb)}
    finally:
        wb.close()
        os.unlink(tmp)


def _parse_allot(wb, ledger):
    """The 'Inventory' pivot: project rows with phase sub-rows, Done /
    Pending / Grand Total unit counts and money columns already in Cr.
    Phase vs project is resolved against the ledger's own structure
    (a label that is a phase of the current project indents under it;
    a repeat, or any other project name, starts a new project)."""
    if "Inventory" not in wb.sheetnames:
        return []
    projset, phases = set(), {}
    for l in ledger:
        projset.add(l["proj"])
        if l["phase"]:
            phases.setdefault(l["proj"], set()).add(_norm_proj(l["phase"]))
    out, cur, used = [], None, set()
    for r in wb["Inventory"].iter_rows(values_only=True):
        lab = _clean(r[0]) if r else None
        if not lab or lab in ("Row Labels",) or lab.startswith("Allotment") or lab.startswith("Count of"):
            continue
        row = {"label": lab, "done": int(_num(r[1])), "pending": int(_num(r[2])),
               "total": int(_num(r[3])), "tcv": _num(r[4]), "called": _num(r[5]),
               "recd": _num(r[6]), "due": _num(r[7]), "fut": _num(r[8])}
        n = _norm_proj(lab)
        if n == "GRAND TOTAL":
            out.append({**row, "kind": "total", "proj": None})
            break
        is_phase = cur is not None and n in phases.get(cur, set()) and n not in used
        if is_phase:
            used.add(n)
            out.append({**row, "kind": "phase", "proj": cur})
        else:
            cur, used = n, set()
            out.append({**row, "kind": "proj", "proj": n})
    return out


def _parse_master(path):
    wb, tmp = _open_copy(path)
    try:
        led_ws = next((wb[n] for n in wb.sheetnames if n.strip().rstrip(".").lower() == "master"), None)
        ledger = _rows_ledger(led_ws) if led_ws is not None else []
        return {"ledger": ledger, "allot": _parse_allot(wb, ledger)}
    finally:
        wb.close()
        os.unlink(tmp)


PARSERS = {"daily": _parse_daily, "master": _parse_master}


def _load(kind):
    path = _find(kind)
    if not path:
        return None, None, f"file not found for '{kind}' in {COLLECTION_DIR}"
    mtime = os.path.getmtime(path)
    with _lock:
        hit = _cache.get(kind)
        if hit and hit[0] == mtime:
            return hit[1], mtime, None
    data = PARSERS[kind](path)
    with _lock:
        _cache[kind] = (mtime, data)
    return data, mtime, None


def register(app):
    @app.after_request
    def _collections_cors(resp):
        # The DASHBOARD_SWD SPA (port 3000) fetches these endpoints
        # cross-origin; data is read-only and already on the intranet.
        if request.path.startswith("/collections"):
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Allow-Headers"] = "*"
        return resp

    @app.route("/collections/data")
    def collections_data():
        try:
            daily, m2, e2 = _load("daily")
            master, m3, e3 = _load("master")
            ledger = (master or {}).get("ledger", [])
            allot = (master or {}).get("allot", [])
            receipts = (daily or {}).get("receipts", [])
            targets = (daily or {}).get("targets", [])
            errors = [e for e in (e2, e3) if e]

            def pack(rows):
                if not rows:
                    return {"cols": [], "rows": []}
                cols = list(rows[0].keys())
                return {"cols": cols, "rows": [[r.get(c) for c in cols] for r in rows]}

            return jsonify({
                "ok": True,
                "asOf": {
                    "daily": datetime.datetime.fromtimestamp(m2).isoformat() if m2 else None,
                    "master": datetime.datetime.fromtimestamp(m3).isoformat() if m3 else None,
                },
                "errors": errors,
                "ledger": pack(ledger),
                "receipts": pack(receipts),
                "targets": pack(targets),
                "allot": pack(allot),
            })
        except Exception as exc:            # noqa: BLE001 — surface to the dashboard
            return jsonify({"ok": False, "error": str(exc)}), 500

    @app.route("/collections/health")
    def collections_health():
        out = {}
        for kind in FILES:
            path = _find(kind)
            out[kind] = {
                "path": path,
                "exists": bool(path and os.path.exists(path)),
                "modified": datetime.datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                if path and os.path.exists(path) else None,
            }
        return jsonify({"ok": True, "dir": COLLECTION_DIR, "files": out})
