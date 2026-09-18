"""
Live PR Report Viewer + SQL Server logger.

Pulls JSON from SapPrReport.php server-side (avoids browser CORS issues),
serves an auto-refreshing HTML table, and simultaneously writes a
timestamped history log into SQL Server (192.168.66.33 / QMS_PR_Report).

Run:
    pip install flask requests pyodbc
    py app.py
Then open http://localhost:5001 (or http://<server-34-ip>:5001) in a browser.
"""

from flask import Flask, jsonify, render_template_string, request
import requests
from datetime import datetime
import os
import sys

# Ensure this script's own folder is importable (needed for embeddable/
# isolated Python distributions, which don't add the script dir automatically).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db_config as cfg

try:
    import db_writer
    _DB_WRITER_AVAILABLE = True
except Exception as _e:
    _DB_WRITER_AVAILABLE = False
    _DB_WRITER_IMPORT_ERROR = _e

try:
    import nfa_tat_writer
    _NFATAT_WRITER_AVAILABLE = True
except Exception as _e2:
    _NFATAT_WRITER_AVAILABLE = False
    _NFATAT_WRITER_IMPORT_ERROR = _e2

app = Flask(__name__)

# PR -> PO journey endpoints (/pr2po/data, /pr2po/health) for the
# DASHBOARD_SWD "PR to PO" page.
import pr2po  # noqa: E402
pr2po.register(app)

# ── Access log (Werkzeug request lines → access.log, rotating 5 MB × 3) ──
import logging
from logging.handlers import RotatingFileHandler as _RFH
_log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "access.log")
_h = _RFH(_log_path, maxBytes=5 * 1024 * 1024, backupCount=3)
_h.setLevel(logging.INFO)
logging.getLogger("werkzeug").addHandler(_h)
logging.getLogger("werkzeug").setLevel(logging.INFO)

# Server-independent access log (Waitress doesn't emit werkzeug lines)
_access_logger = logging.getLogger("qms.access")
_access_logger.setLevel(logging.INFO)
_access_logger.addHandler(_h)
_access_logger.propagate = False

from flask import request as _rq

@app.after_request
def _log_request(response):
    try:
        _access_logger.info(
            '%s - - [%s] "%s %s HTTP/1.1" %s -',
            _rq.remote_addr,
            datetime.now().strftime("%d/%b/%Y %H:%M:%S"),
            _rq.method,
            _rq.full_path.rstrip("?"),
            response.status_code,
        )
    except Exception:
        pass
    return response

PORT = cfg.PORT
REFRESH_SECONDS = 60

INDEX_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{{ title }}</title>
<style>
  * { box-sizing: border-box; }
  body { margin:0; font-family:'Segoe UI', system-ui, -apple-system, sans-serif; background:#f4f6fa; color:#1f2937; }

  /* ---------- header bar ---------- */
  .bar { display:flex; align-items:center; gap:14px; padding:14px 20px; flex-wrap:wrap;
         background:linear-gradient(135deg,#1e3a8a 0%,#2563eb 60%,#3b82f6 100%);
         color:#fff; box-shadow:0 2px 10px rgba(30,58,138,.25); }
  .bar h1 { font-size:17px; margin:0; white-space:nowrap; font-weight:600; letter-spacing:.3px; }
  .bar input[type=text] { background:rgba(255,255,255,.15); border:1px solid rgba(255,255,255,.35);
         color:#fff; border-radius:20px; padding:7px 16px; width:250px; outline:none; transition:.2s; }
  .bar input[type=text]::placeholder { color:rgba(255,255,255,.75); }
  .bar input[type=text]:focus { background:#fff; color:#1f2937; }
  .bar input[type=text]:focus::placeholder { color:#9ca3af; }
  .bar .right { margin-left:auto; display:flex; align-items:center; gap:10px; font-size:12px; }
  .dot { width:8px; height:8px; border-radius:50%; background:#4ade80; display:inline-block; margin-right:5px;
         box-shadow:0 0 6px #4ade80; }
  button, select { background:#fff; border:none; color:#1e3a8a; border-radius:8px; padding:7px 14px;
         cursor:pointer; font-size:12.5px; font-weight:600; box-shadow:0 1px 3px rgba(0,0,0,.15); transition:.15s; }
  button:hover { background:#dbeafe; transform:translateY(-1px); }
  select { font-weight:500; }

  /* ---------- count strip ---------- */
  .count { padding:8px 20px; font-size:12.5px; color:#475569; background:#fff;
         border-bottom:1px solid #e2e8f0; display:flex; gap:18px; align-items:center; flex-wrap:wrap; }
  .count #rowCount { font-weight:700; color:#1e3a8a; background:#dbeafe; padding:3px 12px; border-radius:12px; }
  .hint { color:#94a3b8; font-size:11px; }

  /* ---------- scrollbars ---------- */
  .top-scroll { overflow-x:auto; overflow-y:hidden; height:16px; background:#fff; }
  .top-scroll .spacer { height:1px; }
  .table-wrap { overflow:auto; height:calc(100vh - 128px); background:#fff; }
  ::-webkit-scrollbar { height:12px; width:12px; }
  ::-webkit-scrollbar-thumb { background:#94a3b8; border-radius:6px; border:3px solid #f1f5f9; }
  ::-webkit-scrollbar-thumb:hover { background:#64748b; }
  ::-webkit-scrollbar-track { background:#f1f5f9; }

  /* ---------- table ---------- */
  table { border-collapse:collapse; font-size:12.5px; min-width:100%; }
  th, td { padding:9px 14px; white-space:nowrap; text-align:left; max-width:420px;
         overflow:hidden; text-overflow:ellipsis; }
  td { border-bottom:1px solid #eef2f7; color:#334155; }
  thead th { position:sticky; top:0; background:#1e3a8a; color:#fff; cursor:pointer; z-index:2;
         font-weight:600; font-size:12px; letter-spacing:.3px; border-bottom:2px solid #1e40af; }
  thead th:hover { background:#1e40af; }
  thead tr.filters th { top:36px; cursor:default; padding:6px 10px; z-index:1; background:#eff6ff;
         border-bottom:2px solid #bfdbfe; }
  thead tr.filters input { width:115px; background:#fff; border:1px solid #cbd5e1; color:#1f2937;
         border-radius:6px; padding:4px 8px; font-size:11px; outline:none; transition:.15s; }
  thead tr.filters input:focus { border-color:#2563eb; box-shadow:0 0 0 2px rgba(37,99,235,.15); }
  thead tr.filters input.rng { width:55px; margin-top:3px; }
  tbody tr:nth-child(even) { background:#f8fafc; }
  tbody tr:hover { background:#dbeafe; }
</style>
</head>
<body>
<div class="bar">
  <h1>{{ title }}</h1>
  <input type="text" id="globalFilter" placeholder="Filter all columns...">
  <div class="right">
    <select id="dlScope">
      <option value="filtered">Download: filtered rows</option>
      <option value="latest100">Download: latest 100</option>
      <option value="all">Download: all rows</option>
    </select>
    <button onclick="downloadExcel()">&#11015; Excel</button>
    <button onclick="clearFilters()">Clear filters</button>
    <span><span class="dot"></span>Live &middot; updated <span id="updated">-</span></span>
  </div>
</div>
<div class="count"><span id="rowCount">loading...</span>
  <span class="hint">Column filters: type text to match &middot; numeric/date columns also have min/max range boxes &middot; click a column name to sort</span>
</div>
<div class="top-scroll" id="topScroll"><div class="spacer" id="topSpacer"></div></div>
<div class="table-wrap" id="tableWrap">
  <table>
    <thead>
      <tr id="headRow"></tr>
      <tr class="filters" id="filterRow"></tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
</div>
<script>
const DATA_URL = "{{ data_url }}";
const ONLY_COLS = {{ only_cols | tojson }};
const FIXED_FILTERS = {{ fixed_filters | tojson }};
let rawRows = [], columns = [], colFilters = {}, rangeFilters = {}, sortCol = null, sortDir = 1, numericCols = {};

function isNumericLike(v){ if(v===null||v===undefined||v==='')return false; const t=String(v).replace(/,/g,'').replace(/\s*Lacs?\s*$/i,''); return t!=='' && !isNaN(t); }
function numVal(v){ return parseFloat(String(v).replace(/,/g,'').replace(/\s*Lacs?\s*$/i,'')); }
function isDateLike(v){ return typeof v==='string' && /^\d{4}-\d{2}-\d{2}/.test(v); }

async function load(){
  try{
    const res = await fetch(DATA_URL);
    const data = await res.json();
    let rows = data.rows || data.value || (Array.isArray(data)?data:[]);
    // fixed filters (e.g. Returned view)
    for(const [k,v] of Object.entries(FIXED_FILTERS)) rows = rows.filter(r => String(r[k]??'').trim().toLowerCase() === v.toLowerCase());
    rawRows = rows;
    if(rows.length){
      columns = ONLY_COLS.length ? ONLY_COLS.filter(c=>c in rows[0]) : Object.keys(rows[0]).sort();
      // detect numeric / date columns from a sample
      numericCols = {};
      for(const c of columns){
        let num=0, date=0, n=0;
        for(const r of rows.slice(0,80)){ const v=r[c]; if(v===null||v===''||v===undefined)continue; n++; if(isNumericLike(v))num++; if(isDateLike(v))date++; }
        if(n>0 && date/n>0.7) numericCols[c]='date'; else if(n>0 && num/n>0.7) numericCols[c]='num';
      }
      buildHeader();
    }
    render();
    document.getElementById('updated').textContent = new Date().toLocaleTimeString();
  }catch(e){ console.error(e); }
}

function buildHeader(){
  const hr=document.getElementById('headRow'), fr=document.getElementById('filterRow');
  if(hr.children.length===columns.length) return;
  hr.innerHTML=''; fr.innerHTML='';
  for(const c of columns){
    const th=document.createElement('th'); th.textContent=c;
    th.onclick=()=>{ if(sortCol===c)sortDir*=-1; else {sortCol=c;sortDir=1;} render(); };
    hr.appendChild(th);
    const fth=document.createElement('th');
    const inp=document.createElement('input'); inp.placeholder='filter';
    inp.oninput=()=>{ colFilters[c]=inp.value.toLowerCase(); render(); };
    fth.appendChild(inp);
    if(numericCols[c]){
      const mn=document.createElement('input'); mn.placeholder='min'; mn.className='rng';
      const mx=document.createElement('input'); mx.placeholder='max'; mx.className='rng';
      mn.oninput=()=>{ rangeFilters[c]=rangeFilters[c]||{}; rangeFilters[c].min=mn.value; render(); };
      mx.oninput=()=>{ rangeFilters[c]=rangeFilters[c]||{}; rangeFilters[c].max=mx.value; render(); };
      fth.appendChild(document.createElement('br')); fth.appendChild(mn); fth.appendChild(mx);
    }
    fr.appendChild(fth);
  }
}

function passes(r){
  const g=document.getElementById('globalFilter').value.toLowerCase();
  if(g && !columns.some(c=>String(r[c]??'').toLowerCase().includes(g))) return false;
  for(const [c,f] of Object.entries(colFilters)){ if(f && !String(r[c]??'').toLowerCase().includes(f)) return false; }
  for(const [c,rf] of Object.entries(rangeFilters)){
    const v=r[c]; if(v===null||v===undefined||v==='') { if(rf.min||rf.max) return false; continue; }
    if(numericCols[c]==='date'){ const s=String(v).slice(0,10); if(rf.min && s<rf.min) return false; if(rf.max && s>rf.max) return false; }
    else { const n=numVal(v); if(isNaN(n)) return false; if(rf.min!=='' && rf.min!==undefined && n<parseFloat(rf.min)) return false; if(rf.max!=='' && rf.max!==undefined && n>parseFloat(rf.max)) return false; }
  }
  return true;
}

function filteredRows(){
  let rows = rawRows.filter(passes);
  if(sortCol){
    rows=[...rows].sort((a,b)=>{ const x=a[sortCol]??'', y=b[sortCol]??'';
      if(numericCols[sortCol]==='num') return (numVal(x)-numVal(y))*sortDir;
      return String(x).localeCompare(String(y))*sortDir; });
  }
  return rows;
}

function render(){
  const rows=filteredRows();
  const tb=document.getElementById('tbody'); tb.innerHTML='';
  const frag=document.createDocumentFragment();
  for(const r of rows){
    const tr=document.createElement('tr');
    for(const c of columns){ const td=document.createElement('td'); const v=r[c]; td.textContent=(v===null||v===undefined)?'':v; td.title=td.textContent; tr.appendChild(td); }
    frag.appendChild(tr);
  }
  tb.appendChild(frag);
  document.getElementById('rowCount').textContent = rows.length+' of '+rawRows.length+' rows';
  document.getElementById('topSpacer').style.width=document.getElementById('tableWrap').scrollWidth+'px';
}

function clearFilters(){
  colFilters={}; rangeFilters={}; document.getElementById('globalFilter').value='';
  document.querySelectorAll('#filterRow input').forEach(i=>i.value='');
  render();
}

function downloadExcel(){
  const scope=document.getElementById('dlScope').value;
  let rows = scope==='all' ? rawRows : filteredRows();
  if(scope==='latest100') rows = rows.slice(0,100);
  const esc=v=>{ v=(v===null||v===undefined)?'':String(v); return '"'+v.replace(/"/g,'""')+'"'; };
  let csv='\uFEFF'+columns.map(esc).join(',')+'\n';
  for(const r of rows) csv+=columns.map(c=>esc(r[c])).join(',')+'\n';
  const blob=new Blob([csv],{type:'text/csv;charset=utf-8;'});
  const a=document.createElement('a'); a.href=URL.createObjectURL(blob);
  a.download='{{ title }}'.replace(/\s+/g,'_')+'_'+new Date().toISOString().slice(0,10)+'.csv';
  a.click(); URL.revokeObjectURL(a.href);
}

// synced top scrollbar
const topScroll=document.getElementById('topScroll'), tableWrap=document.getElementById('tableWrap');
let syncing=false;
topScroll.addEventListener('scroll',()=>{ if(syncing){syncing=false;return;} syncing=true; tableWrap.scrollLeft=topScroll.scrollLeft; });
tableWrap.addEventListener('scroll',()=>{ if(syncing){syncing=false;return;} syncing=true; topScroll.scrollLeft=tableWrap.scrollLeft; });

document.getElementById('globalFilter').addEventListener('input', render);
load();
setInterval(load, {{ refresh_seconds }} * 1000);
</script>
</body>
</html>
"""


def normalize_rows(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "rows", "result", "results", "d"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return [data]
    return []


@app.route("/health")
def health():
    """One-glance health: DB reachable + row counts + last write times."""
    out = {"app": "ok", "server_time": datetime.now().isoformat()}
    try:
        import pyodbc
        conn = pyodbc.connect(
            f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
            f"DATABASE={cfg.DB_NAME};Trusted_Connection=yes;TrustServerCertificate=yes;",
            timeout=5,
        )
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*), MAX(fetched_at) FROM [dbo].[{cfg.TABLE_NAME}]")
        pr_rows, pr_last = cur.fetchone()
        cur.execute(f"SELECT COUNT(*), MAX(fetched_at) FROM [dbo].[{cfg.NFATAT_TABLE_NAME}]")
        nf_rows, nf_last = cur.fetchone()
        conn.close()
        out["db"] = "ok"
        out["pr_table"] = {"rows": pr_rows, "last_write": str(pr_last)}
        out["nfatat_table"] = {"rows": nf_rows, "last_write": str(nf_last)}
        return jsonify(out), 200
    except Exception as e:
        out["db"] = "error"
        out["error"] = str(e)
        return jsonify(out), 503


def _table_page(title, data_url, only_cols=None, fixed_filters=None):
    return render_template_string(
        INDEX_HTML, refresh_seconds=REFRESH_SECONDS, title=title,
        data_url=data_url, only_cols=only_cols or [],
        fixed_filters=fixed_filters or {},
    )


@app.route("/")
def index():
    return _table_page("Live PR Report", "/db/pr")


@app.route("/nfatat")
def nfatat_index():
    return _table_page("Live NFA TAT Report", "/db/nfatat")


@app.route("/nfatat/returned")
def nfatat_returned():
    """Focused view: Returned PRs only, key workflow columns."""
    return _table_page(
        "Returned PRs - NFA TAT",
        "/db/nfatat",
        only_cols=["EPR_No", "PRH_Status", "PRH_Status_Desc", "CP_Team_Date",
                   "Assignee_Team_Date", "Assignee_Team_Msg", "CP_Team_Msg"],
        fixed_filters={"PRH_Status_Desc": "Returned"},
    )


def _db_rows(table_name):
    """All rows from our SQL table as a list of dicts (instant, no vendor call)."""
    import pyodbc
    conn = pyodbc.connect(
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
        f"DATABASE={cfg.DB_NAME};Trusted_Connection=yes;TrustServerCertificate=yes;",
        timeout=8,
    )
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM [dbo].[{table_name}] ORDER BY fetched_at DESC")
        cols = [d[0] for d in cur.description]
        hidden = {"fetched_at", "first_seen"}  # internal sync metadata
        return [
            {c: (str(v) if v is not None else None)
             for c, v in zip(cols, row) if c not in hidden}
            for row in cur.fetchall()
        ]
    finally:
        conn.close()


@app.route("/db/pr")
def db_pr():
    """PR table from OUR database - instant, synced every 45s."""
    try:
        rows = _db_rows(cfg.TABLE_NAME)
        return jsonify({"ok": True, "rows": rows, "count": len(rows), "source": "db"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200


@app.route("/db/nfatat")
def db_nfatat():
    """NFA TAT table from OUR database - instant, synced every 5 min."""
    try:
        rows = _db_rows(cfg.NFATAT_TABLE_NAME)
        return jsonify({"ok": True, "rows": rows, "count": len(rows), "source": "db"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200


@app.route("/api/data")
def api_data():
    try:
        resp = requests.get(cfg.SOURCE_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        rows = normalize_rows(data)
        return jsonify({"ok": True, "rows": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200


@app.route("/nfatat/data")
def nfatat_data():
    """Full NFA TAT report (rolling window), flat JSON like /api/data.
    Optional overrides: ?startdate=YYYY-MM-DD&enddate=YYYY-MM-DD"""
    try:
        from datetime import timedelta
        end = request.args.get("enddate") or datetime.now().date().isoformat()
        start = request.args.get("startdate") or (
            datetime.now().date() - timedelta(days=cfg.NFATAT_ROLLING_DAYS)
        ).isoformat()
        url = f"{cfg.NFATAT_SOURCE_URL_BASE}?startdate={start}&enddate={end}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        rows = normalize_rows(resp.json())
        return jsonify({"ok": True, "rows": rows, "count": len(rows),
                        "startdate": start, "enddate": end})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200


@app.route("/odata/PRReportHistory")
def odata_pr_report_history():
    """
    OData-v4-style JSON feed over the SQL Server history table, for SAP
    Gateway / HTTP destinations to consume directly.

    Query params:
        $top   - max rows to return (default 100, max 5000)
        $skip  - rows to skip (for paging)
    Rows are returned newest-first (by fetched_at).
    """
    if not _DB_WRITER_AVAILABLE:
        return jsonify({"error": "Database module unavailable on server"}), 500

    try:
        top = min(int(request.args.get("$top", 100)), 5000)
        skip = int(request.args.get("$skip", 0))
    except ValueError:
        return jsonify({"error": "$top and $skip must be integers"}), 400

    try:
        import pyodbc
        conn = pyodbc.connect(db_writer._db_connection_string(), autocommit=True)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT * FROM [dbo].[{cfg.TABLE_NAME}] "
                f"ORDER BY fetched_at DESC "
                f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                skip, top,
            )
            columns = [c[0] for c in cur.description]
            rows = []
            for record in cur.fetchall():
                row = {}
                for col, val in zip(columns, record):
                    if isinstance(val, datetime):
                        val = val.isoformat()
                    row[col] = val
                rows.append(row)
        finally:
            conn.close()

        base = request.url_root.rstrip("/")
        return jsonify({
            "@odata.context": f"{base}/odata/$metadata#PRReportHistory",
            "value": rows,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/check_pr")
def check_pr():
    """
    Checks whether a given PR number exists in the SQL Server history
    table. Returns a structured JSON payload for SAP/ABAP to deserialize.

    Usage:
        http://<server>:5002/check_pr?pr=8110000659

    Response shape (found):
        {
          "PR_Number": "8110000659",
          "Status": "S",
          "Found": true,
          "Message": "PR number found",
          "CheckedAt": "2026-08-11T11:20:03",
          "Details": { ...full matching row, most recent fetch... }
        }

    Response shape (not found / error / missing param):
        {
          "PR_Number": "8110000659",
          "Status": "E",
          "Found": false,
          "Message": "PR number not found",
          "CheckedAt": "2026-08-11T11:20:03",
          "Details": null
        }
    """
    pr_number = request.args.get("pr", "").strip()
    checked_at = datetime.now().isoformat()

    def make_response(status, found, message, details=None, http_code=200):
        return jsonify({
            "PR_Number": pr_number or None,
            "Status": status,
            "Found": found,
            "Message": message,
            "CheckedAt": checked_at,
            "Details": details,
        }), http_code

    if not pr_number:
        return make_response("E", False, "Missing required parameter 'pr'", http_code=400)

    if not _DB_WRITER_AVAILABLE:
        return make_response("E", False, "Database module unavailable on server", http_code=500)

    try:
        import pyodbc
        conn = pyodbc.connect(db_writer._db_connection_string(), autocommit=True)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT TOP 1 * FROM [dbo].[{cfg.TABLE_NAME}] "
                f"WHERE [PR_No] = ? ORDER BY fetched_at DESC",
                pr_number,
            )
            columns = [c[0] for c in cur.description]
            record = cur.fetchone()
        finally:
            conn.close()

        if record is not None:
            details = {}
            for col, val in zip(columns, record):
                if isinstance(val, datetime):
                    val = val.isoformat()
                details[col] = val
            return make_response("S", True, "PR number found", details=details)
        else:
            return make_response("E", False, "PR number not found")

    except Exception as e:
        print(f"[check_pr] ERROR checking PR {pr_number}: {e}")
        return make_response("E", False, "Internal error while checking PR", http_code=500)


@app.route("/nfatat/odata/PRNFATatReportHistory")
def odata_nfatat_history():
    """
    OData-v4-style JSON feed over the PR/NFA TAT history table.

    Query params:
        $top   - max rows to return (default 100, max 5000)
        $skip  - rows to skip (for paging)
    Rows are returned newest-first (by fetched_at).
    """
    if not _NFATAT_WRITER_AVAILABLE:
        return jsonify({"error": "NFA TAT module unavailable on server"}), 500

    try:
        top = min(int(request.args.get("$top", 100)), 5000)
        skip = int(request.args.get("$skip", 0))
    except ValueError:
        return jsonify({"error": "$top and $skip must be integers"}), 400

    try:
        import pyodbc
        conn = pyodbc.connect(nfa_tat_writer._db_connection_string(), autocommit=True)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT * FROM [dbo].[{cfg.NFATAT_TABLE_NAME}] "
                f"ORDER BY fetched_at DESC "
                f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                skip, top,
            )
            columns = [c[0] for c in cur.description]
            rows = []
            for record in cur.fetchall():
                row = {}
                for col, val in zip(columns, record):
                    if isinstance(val, datetime):
                        val = val.isoformat()
                    row[col] = val
                rows.append(row)
        finally:
            conn.close()

        base = request.url_root.rstrip("/")
        return jsonify({
            "@odata.context": f"{base}/nfatat/odata/$metadata#PRNFATatReportHistory",
            "value": rows,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/nfatat/check_pr")
def check_pr_nfatat():
    """
    Checks whether a given PR number exists in the PR/NFA TAT history
    table (rolling window, see NFATAT_ROLLING_DAYS in db_config.py).
    Returns a structured JSON payload, same shape as /check_pr.

    Usage:
        http://<server>:5002/nfatat/check_pr?pr=8110000659

    NOTE: NFATAT_PR_COLUMN in db_config.py is a best guess ("PR_No").
    Verify the actual column name in SSMS after the first run and
    update db_config.py if it's different.
    """
    pr_number = request.args.get("pr", "").strip()
    checked_at = datetime.now().isoformat()

    def make_response(status, found, message, details=None, http_code=200):
        return jsonify({
            "PR_Number": pr_number or None,
            "Status": status,
            "Found": found,
            "Message": message,
            "CheckedAt": checked_at,
            "Details": details,
        }), http_code

    if not pr_number:
        return make_response("E", False, "Missing required parameter 'pr'", http_code=400)

    if not _NFATAT_WRITER_AVAILABLE:
        return make_response("E", False, "NFA TAT module unavailable on server", http_code=500)

    try:
        import pyodbc
        conn = pyodbc.connect(nfa_tat_writer._db_connection_string(), autocommit=True)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT TOP 1 * FROM [dbo].[{cfg.NFATAT_TABLE_NAME}] "
                f"WHERE [{cfg.NFATAT_PR_COLUMN}] = ? ORDER BY fetched_at DESC",
                pr_number,
            )
            columns = [c[0] for c in cur.description]
            record = cur.fetchone()
        finally:
            conn.close()

        if record is not None:
            details = {}
            for col, val in zip(columns, record):
                if isinstance(val, datetime):
                    val = val.isoformat()
                details[col] = val
            return make_response("S", True, "PR number found", details=details)
        else:
            return make_response("E", False, "PR number not found")

    except Exception as e:
        print(f"[check_pr_nfatat] ERROR checking PR {pr_number}: {e}")
        return make_response("E", False, "Internal error while checking PR", http_code=500)


@app.route("/nfatat/search")
def nfatat_search():
    """
    Combined filter endpoint for the PR/NFA TAT history table.

    Rules:
      - If 'pr' is given: returns that specific PR's most recent record
        (S/E style payload), same as /nfatat/check_pr. If a date range
        is ALSO given, the PR lookup is additionally restricted to that
        window.
      - Else if 'startdate' and/or 'enddate' is given (no 'pr'): returns
        ALL PR records captured within that date range (based on
        fetched_at, i.e. when this app captured the data -- not
        necessarily a business date field in the source report).
      - If neither is given: returns an error asking for at least one.

    Usage:
        http://<server>:5002/nfatat/search?pr=8110000659
        http://<server>:5002/nfatat/search?startdate=2026-08-01&enddate=2026-08-17
        http://<server>:5002/nfatat/search?pr=8110000659&startdate=2026-08-01&enddate=2026-08-17

    Dates must be YYYY-MM-DD. $top caps date-range results (default 500, max 5000).
    """
    pr_number = request.args.get("pr", "").strip()
    startdate = request.args.get("startdate", "").strip()
    enddate = request.args.get("enddate", "").strip()
    checked_at = datetime.now().isoformat()

    def parse_date(s, label):
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"'{label}' must be in YYYY-MM-DD format, got '{s}'")

    if not _NFATAT_WRITER_AVAILABLE:
        return jsonify({"error": "NFA TAT module unavailable on server"}), 500

    try:
        start_dt = parse_date(startdate, "startdate")
        end_dt = parse_date(enddate, "enddate")
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # --- Case 1: PR number given -> single-PR lookup, optionally date-bounded ---
    if pr_number:
        def make_pr_response(status, found, message, details=None, http_code=200):
            return jsonify({
                "Filter": "pr",
                "PR_Number": pr_number,
                "Status": status,
                "Found": found,
                "Message": message,
                "CheckedAt": checked_at,
                "Details": details,
            }), http_code

        try:
            import pyodbc
            conn = pyodbc.connect(nfa_tat_writer._db_connection_string(), autocommit=True)
            try:
                cur = conn.cursor()
                where = [f"[{cfg.NFATAT_PR_COLUMN}] = ?"]
                params = [pr_number]
                if start_dt:
                    where.append("fetched_at >= ?")
                    params.append(start_dt)
                if end_dt:
                    where.append("fetched_at < DATEADD(day, 1, ?)")
                    params.append(end_dt)
                query = (
                    f"SELECT TOP 1 * FROM [dbo].[{cfg.NFATAT_TABLE_NAME}] "
                    f"WHERE {' AND '.join(where)} ORDER BY fetched_at DESC"
                )
                cur.execute(query, params)
                columns = [c[0] for c in cur.description]
                record = cur.fetchone()
            finally:
                conn.close()

            if record is not None:
                details = {}
                for col, val in zip(columns, record):
                    if isinstance(val, datetime):
                        val = val.isoformat()
                    details[col] = val
                return make_pr_response("S", True, "PR number found", details=details)
            else:
                return make_pr_response("E", False, "PR number not found")

        except Exception as e:
            print(f"[nfatat_search] ERROR checking PR {pr_number}: {e}")
            return make_pr_response("E", False, "Internal error while checking PR", http_code=500)

    # --- Case 2: date range given, no PR -> all PRs in that window ---
    elif start_dt or end_dt:
        try:
            top = min(int(request.args.get("$top", 500)), 5000)
        except ValueError:
            return jsonify({"error": "$top must be an integer"}), 400

        try:
            import pyodbc
            conn = pyodbc.connect(nfa_tat_writer._db_connection_string(), autocommit=True)
            try:
                cur = conn.cursor()
                where = []
                params = []
                if start_dt:
                    where.append("fetched_at >= ?")
                    params.append(start_dt)
                if end_dt:
                    where.append("fetched_at < DATEADD(day, 1, ?)")
                    params.append(end_dt)
                query = (
                    f"SELECT TOP {top} * FROM [dbo].[{cfg.NFATAT_TABLE_NAME}] "
                    f"WHERE {' AND '.join(where)} ORDER BY fetched_at DESC"
                )
                cur.execute(query, params)
                columns = [c[0] for c in cur.description]
                rows = []
                for record in cur.fetchall():
                    row = {}
                    for col, val in zip(columns, record):
                        if isinstance(val, datetime):
                            val = val.isoformat()
                        row[col] = val
                    rows.append(row)
            finally:
                conn.close()

            return jsonify({
                "Filter": "date",
                "StartDate": startdate or None,
                "EndDate": enddate or None,
                "CheckedAt": checked_at,
                "Count": len(rows),
                "Rows": rows,
            })
        except Exception as e:
            print(f"[nfatat_search] ERROR on date range search: {e}")
            return jsonify({"error": str(e)}), 500

    # --- Case 3: neither given ---
    else:
        return jsonify({
            "error": "Provide at least one filter: 'pr', or 'startdate'/'enddate'"
        }), 400


if __name__ == "__main__":
    if _DB_WRITER_AVAILABLE:
        try:
            db_writer.start_background_thread()
        except Exception as e:
            print(f"[db_writer] Failed to start: {e}")
            print("[db_writer] The live table will still work; SQL logging is disabled.")
    else:
        print(f"[db_writer] Not available ({_DB_WRITER_IMPORT_ERROR}). "
              f"Run: pip install pyodbc")

    if _NFATAT_WRITER_AVAILABLE:
        try:
            nfa_tat_writer.start_background_thread()
        except Exception as e:
            print(f"[nfa_tat_writer] Failed to start: {e}")
    else:
        print(f"[nfa_tat_writer] Not available ({_NFATAT_WRITER_IMPORT_ERROR})")

    print(f"Serving live PR report on http://localhost:{PORT}")
    try:
        from waitress import serve
        print(f"[server] Waitress production WSGI server, 8 threads, port {PORT}")
        serve(app, host="0.0.0.0", port=PORT, threads=8,
              connection_limit=200, channel_timeout=60)
    except ImportError:
        print("[server] WARNING: waitress not installed - falling back to Flask dev server. "
              "Run: pip install waitress")
        app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
