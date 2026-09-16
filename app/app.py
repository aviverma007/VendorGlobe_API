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
REFRESH_SECONDS = 5

INDEX_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Live PR Report</title>
<style>
  :root {
    --bg: #0f1115;
    --panel: #161a21;
    --border: #2a2f3a;
    --text: #e6e9ef;
    --muted: #8b93a5;
    --accent: #4f9dff;
    --row-alt: #12151b;
    --row-hover: #1c2129;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: 'Segoe UI', Roboto, Arial, sans-serif;
    font-size: 13px;
  }
  header {
    position: sticky;
    top: 0;
    z-index: 10;
    background: var(--panel);
    border-bottom: 1px solid var(--border);
    padding: 12px 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 10px;
  }
  header h1 { font-size: 16px; margin: 0; font-weight: 600; }
  .status { display: flex; align-items: center; gap: 8px; color: var(--muted); }
  .dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: #3ddc84; box-shadow: 0 0 6px #3ddc84;
    animation: pulse 1.5s infinite;
  }
  .dot.error { background: #ff5c5c; box-shadow: 0 0 6px #ff5c5c; animation: none; }
  @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; } }
  #search {
    background: var(--bg); border: 1px solid var(--border); color: var(--text);
    padding: 6px 10px; border-radius: 6px; width: 240px;
  }
  #search:focus { outline: none; border-color: var(--accent); }
  .table-wrap { overflow: auto; height: calc(100vh - 58px); }
  table { border-collapse: collapse; width: 100%; white-space: nowrap; }
  thead th {
    position: sticky; top: 0; background: var(--panel); color: var(--accent);
    text-align: left; padding: 8px 12px; border-bottom: 2px solid var(--border);
    font-weight: 600; z-index: 5; cursor: pointer; user-select: none;
  }
  thead th:hover { color: #fff; }
  tbody td { padding: 6px 12px; border-bottom: 1px solid var(--border); }
  tbody tr:nth-child(even) { background: var(--row-alt); }
  tbody tr:hover { background: var(--row-hover); }
  .meta { color: var(--muted); padding: 10px 20px; }
  .err-banner { color: #ff5c5c; padding: 10px 20px; display: none; }
</style>
</head>
<body>
<header>
  <h1>Live PR Report</h1>
  <input id="search" type="text" placeholder="Filter rows..." />
  <div class="status">
    <span class="dot" id="dot"></span>
    <span id="statusText">Loading...</span>
  </div>
</header>
<div class="err-banner" id="errBanner"></div>
<div class="meta" id="meta"></div>
<div class="table-wrap">
  <table>
    <thead><tr id="headRow"></tr></thead>
    <tbody id="body"></tbody>
  </table>
</div>

<script>
const REFRESH_MS = {{ refresh_seconds }} * 1000;
let rawRows = [];
let columns = [];
let sortCol = null;
let sortDir = 1;

function setStatus(ok, text) {
  document.getElementById('dot').className = ok ? 'dot' : 'dot error';
  document.getElementById('statusText').textContent = text;
}

function renderHead() {
  const headRow = document.getElementById('headRow');
  headRow.innerHTML = '';
  columns.forEach(col => {
    const th = document.createElement('th');
    th.textContent = col + (sortCol === col ? (sortDir === 1 ? ' \u25B2' : ' \u25BC') : '');
    th.onclick = () => {
      if (sortCol === col) { sortDir *= -1; } else { sortCol = col; sortDir = 1; }
      renderHead();
      renderBody();
    };
    headRow.appendChild(th);
  });
}

function renderBody() {
  const filter = document.getElementById('search').value.toLowerCase();
  let rows = rawRows.filter(r => !filter || JSON.stringify(r).toLowerCase().includes(filter));

  if (sortCol) {
    rows = rows.slice().sort((a, b) => {
      const av = (a[sortCol] ?? '').toString();
      const bv = (b[sortCol] ?? '').toString();
      const an = parseFloat(av), bn = parseFloat(bv);
      let cmp;
      if (!isNaN(an) && !isNaN(bn) && av.trim() !== '' && bv.trim() !== '') {
        cmp = an - bn;
      } else {
        cmp = av.localeCompare(bv);
      }
      return cmp * sortDir;
    });
  }

  const body = document.getElementById('body');
  body.innerHTML = '';
  const frag = document.createDocumentFragment();
  rows.forEach(row => {
    const tr = document.createElement('tr');
    columns.forEach(col => {
      const td = document.createElement('td');
      const val = row[col];
      td.textContent = (val === null || val === undefined) ? '' : val;
      tr.appendChild(td);
    });
    frag.appendChild(tr);
  });
  body.appendChild(frag);

  document.getElementById('meta').textContent =
    `${rows.length} of ${rawRows.length} rows` + (filter ? ' (filtered)' : '');
}

async function refresh() {
  try {
    const res = await fetch('/api/data');
    const json = await res.json();
    if (!json.ok) throw new Error(json.error || 'Unknown error');

    rawRows = json.rows;
    if (rawRows.length > 0) {
      const newColumns = Object.keys(rawRows[0]);
      if (JSON.stringify(newColumns) !== JSON.stringify(columns)) {
        columns = newColumns;
        renderHead();
      }
    }
    renderBody();
    document.getElementById('errBanner').style.display = 'none';
    setStatus(true, 'Live \u00b7 updated ' + new Date().toLocaleTimeString());
  } catch (e) {
    setStatus(false, 'Error \u00b7 retrying...');
    const banner = document.getElementById('errBanner');
    banner.textContent = 'Fetch failed: ' + e.message;
    banner.style.display = 'block';
  }
}

document.getElementById('search').addEventListener('input', renderBody);

refresh();
setInterval(refresh, REFRESH_MS);
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


@app.route("/")
def index():
    return render_template_string(INDEX_HTML, refresh_seconds=REFRESH_SECONDS)


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
