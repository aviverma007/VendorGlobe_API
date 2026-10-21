"""
DASHBOARD_SWD logins and user rights (server side).

Replaces the hard-coded user list that used to ship inside the dashboard's
JavaScript bundle. Users, password hashes and sessions live in SQL Server:

    dbo.DASH_USERS     UserId (lower-case, PK), DisplayName, Role,
                       Access ('all' or JSON list of route paths),
                       PwHash (pbkdf2_sha256), Active, MustChange, audit cols
    dbo.DASH_SESSIONS  TokenHash (sha256 of the bearer token, PK), UserId,
                       CreatedAt, ExpiresAt, LastSeen

Endpoints (all JSON, CORS open like the other dashboard feeds):
    POST /auth/login             {id, password} -> {token, user}
    GET  /auth/me                bearer -> {user}      (fresh rights on every app load)
    POST /auth/logout            bearer
    POST /auth/change-password   bearer, {current, new}
  admin role only:
    GET  /auth/users
    POST /auth/users                         {id, displayName, role, access, password, mustChange}
    PUT  /auth/users/<id>                    {displayName?, role?, access?, active?}
    POST /auth/users/<id>/reset-password     {password?, mustChange?}  -> {tempPassword?}
         (blank password = generate one; it is returned once and never stored in clear)

The first start seeds DASH_USERS from SEED_USERS (the logins the dashboard
had before) when the table is empty, so nobody is locked out.
"""
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
from datetime import datetime, timedelta

import pyodbc
from flask import jsonify, request

import db_config as cfg

DB_NAME = os.environ.get("VG_AUTH_DB", cfg.DB_NAME)
SESSION_DAYS = int(os.environ.get("VG_AUTH_SESSION_DAYS", "30"))
PBKDF2_ITERS = 200_000
ROLES = ["admin", "developer", "sales", "finance", "management", "crm", "pl"]
MIN_PW = 6

# Logins that existed in the dashboard bundle (users.ts) before this service.
# Used ONCE, only when DASH_USERS is empty; hashed on insert. Change these
# passwords from Settings -> User rights after go-live.
_SALES = ["/overview", "/bookings", "/eoi", "/target", "/channel-partners", "/gallery-footfall", "/digital-leads"]
SEED_USERS = [
    ("admin@admin", "admin", "Admin", "admin", "all"),
    ("anirudh", "swd@2026", "Anirudh", "developer", "all"),
    ("sales", "sales@123", "Sales Team", "sales", "all"),
    ("finance", "finance@123", "Finance Team", "finance", "all"),
    ("management", "mgmt@123", "Management", "management", "all"),
    ("amit.sharma@smartworlddevelopers.com", "Swd@2026", "Amit Sharma", "management", "all"),
    ("sales@smartworlddevelopers.com", "Swd@2026", "Sales", "sales", _SALES),
    ("crm@smartworlddevelopers.com", "Swd@2026", "CRM", "crm",
     _SALES + ["/case-management", "/loan-details", "/collections", "/sap-collections"]),
    ("p&l@smartworlddevelopers.com", "Swd@2026", "P&L", "pl", ["/overview", "/target"]),
    ("procurement@smartworlddevelopers.com", "Swd@2026", "Procurement", "finance", ["/pr-to-po", "/cost"]),
]

_ready = False
_ready_lock = threading.Lock()
_fails = {}            # (ip, id) -> [timestamps] for login throttling
FAIL_LIMIT, FAIL_WINDOW = 6, 10 * 60


def _log(msg):
    print(f"[dash_auth] {datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


def _connect():
    return pyodbc.connect(
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
        f"DATABASE={DB_NAME};Trusted_Connection=yes;TrustServerCertificate=yes;",
        autocommit=True,
    )


# ---------------------------------------------------------------- crypto --

def hash_pw(pw):
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), bytes.fromhex(salt), PBKDF2_ITERS)
    return f"pbkdf2_sha256${PBKDF2_ITERS}${salt}${dk.hex()}"


def check_pw(pw, stored):
    try:
        algo, iters, salt, hx = stored.split("$")
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), bytes.fromhex(salt), int(iters))
        return hmac.compare_digest(dk.hex(), hx)
    except Exception:   # noqa: BLE001
        return False


def _tok_hash(tok):
    return hashlib.sha256(tok.encode("utf-8")).hexdigest()


def gen_temp_password():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789"
    return "".join(secrets.choice(alphabet) for _ in range(10))


# ---------------------------------------------------------------- tables --

def _ensure(cur):
    cur.execute("""
        IF OBJECT_ID('dbo.DASH_USERS','U') IS NULL
        CREATE TABLE dbo.DASH_USERS (
            UserId NVARCHAR(200) NOT NULL PRIMARY KEY,
            DisplayName NVARCHAR(200) NOT NULL,
            Role NVARCHAR(40) NOT NULL,
            Access NVARCHAR(MAX) NOT NULL,
            PwHash NVARCHAR(400) NOT NULL,
            Active BIT NOT NULL DEFAULT 1,
            MustChange BIT NOT NULL DEFAULT 0,
            CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            UpdatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            UpdatedBy NVARCHAR(200) NULL,
            LastLogin DATETIME2 NULL
        )""")
    cur.execute("""
        IF OBJECT_ID('dbo.DASH_SESSIONS','U') IS NULL
        CREATE TABLE dbo.DASH_SESSIONS (
            TokenHash CHAR(64) NOT NULL PRIMARY KEY,
            UserId NVARCHAR(200) NOT NULL,
            CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            ExpiresAt DATETIME2 NOT NULL,
            LastSeen DATETIME2 NULL
        )""")


def _setup():
    global _ready
    if _ready:
        return
    with _ready_lock:
        if _ready:
            return
        conn = _connect(); cur = conn.cursor()
        _ensure(cur)
        cur.execute("SELECT COUNT(*) FROM dbo.DASH_USERS")
        if cur.fetchone()[0] == 0:
            for uid, pw, name, role, acc in SEED_USERS:
                cur.execute("INSERT INTO dbo.DASH_USERS (UserId, DisplayName, Role, Access, PwHash, UpdatedBy) VALUES (?,?,?,?,?,?)",
                            uid.lower(), name, role, _acc_out(acc), hash_pw(pw), "seed")
            _log(f"seeded {len(SEED_USERS)} users")
        cur.execute("DELETE FROM dbo.DASH_SESSIONS WHERE ExpiresAt < SYSUTCDATETIME()")
        conn.close()
        _ready = True


def _acc_out(acc):
    return "all" if acc == "all" else json.dumps(sorted(set(acc)))


def _acc_in(raw):
    if raw is None or raw == "all":
        return "all"
    try:
        v = json.loads(raw)
        return v if isinstance(v, list) else "all"
    except Exception:   # noqa: BLE001
        return []


def _row_user(r):
    uid, name, role, acc, _pw, active, must, created, updated, upd_by, last = r
    return {"id": uid, "displayName": name, "role": role, "access": _acc_in(acc), "active": bool(active),
            "mustChange": bool(must), "createdAt": str(created)[:19], "updatedAt": str(updated)[:19],
            "updatedBy": upd_by, "lastLogin": str(last)[:19] if last else None}


COLS = "UserId, DisplayName, Role, Access, PwHash, Active, MustChange, CreatedAt, UpdatedAt, UpdatedBy, LastLogin"


def _get_user(cur, uid):
    cur.execute(f"SELECT {COLS} FROM dbo.DASH_USERS WHERE UserId = ?", uid.strip().lower())
    return cur.fetchone()


def _public(u):
    return {k: u[k] for k in ("id", "displayName", "role", "access", "mustChange")}


# ---------------------------------------------------------------- sessions --

def _session_user(cur):
    h = request.headers.get("Authorization", "")
    if not h.startswith("Bearer "):
        return None
    th = _tok_hash(h[7:].strip())
    cur.execute("SELECT UserId FROM dbo.DASH_SESSIONS WHERE TokenHash = ? AND ExpiresAt > SYSUTCDATETIME()", th)
    r = cur.fetchone()
    if not r:
        return None
    row = _get_user(cur, r[0])
    if not row or not row[5]:          # deleted or disabled -> session is dead
        cur.execute("DELETE FROM dbo.DASH_SESSIONS WHERE TokenHash = ?", th)
        return None
    cur.execute("UPDATE dbo.DASH_SESSIONS SET LastSeen = SYSUTCDATETIME() WHERE TokenHash = ?", th)
    return _row_user(row)


def _err(msg, code):
    return jsonify({"ok": False, "error": msg}), code


def _clean_access(acc):
    if acc == "all":
        return "all"
    if not isinstance(acc, list):
        raise ValueError("access must be 'all' or a list of tab paths")
    out = sorted({str(p).strip() for p in acc if str(p).strip().startswith("/")})
    return out


def _admins_left(cur, excluding):
    cur.execute("SELECT COUNT(*) FROM dbo.DASH_USERS WHERE Role = 'admin' AND Active = 1 AND UserId <> ?", excluding)
    return cur.fetchone()[0]


# ---------------------------------------------------------------- routes --

def register(app):
    @app.after_request
    def _auth_cors(resp):
        if request.path.startswith("/auth/"):
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
            resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, OPTIONS"
            resp.headers["Cache-Control"] = "no-store"
        return resp

    def route(rule, methods):
        """Register with OPTIONS (CORS preflight) answered before any DB work."""
        def deco(fn):
            def wrapped(*a, **kw):
                if request.method == "OPTIONS":
                    return ("", 204)
                try:
                    _setup()
                    return fn(*a, **kw)
                except pyodbc.Error as e:
                    _log(f"db error on {rule}: {e}")
                    return _err("User database unreachable", 503)
            wrapped.__name__ = "auth_" + fn.__name__
            app.add_url_rule(rule, wrapped.__name__, wrapped, methods=methods + ["OPTIONS"])
            return wrapped
        return deco

    @route("/auth/login", ["POST"])
    def login():
        body = request.get_json(silent=True) or {}
        uid = str(body.get("id", "")).strip().lower()
        pw = str(body.get("password", ""))
        key = (request.remote_addr, uid)
        now = time.time()
        recent = [t for t in _fails.get(key, []) if now - t < FAIL_WINDOW]
        if len(recent) >= FAIL_LIMIT:
            return _err("Too many wrong attempts. Try again in 10 minutes.", 429)
        conn = _connect(); cur = conn.cursor()
        row = _get_user(cur, uid) if uid else None
        if not row or not check_pw(pw, row[4]):
            recent.append(now); _fails[key] = recent
            conn.close()
            return _err("Incorrect user ID or password.", 401)
        if not row[5]:
            conn.close()
            return _err("This user ID is disabled. Ask the admin to enable it.", 403)
        _fails.pop(key, None)
        tok = secrets.token_urlsafe(32)
        cur.execute("INSERT INTO dbo.DASH_SESSIONS (TokenHash, UserId, ExpiresAt) VALUES (?,?,?)",
                    _tok_hash(tok), row[0], datetime.utcnow() + timedelta(days=SESSION_DAYS))
        cur.execute("UPDATE dbo.DASH_USERS SET LastLogin = SYSUTCDATETIME() WHERE UserId = ?", row[0])
        conn.close()
        return jsonify({"ok": True, "token": tok, "user": _public(_row_user(row))})

    @route("/auth/me", ["GET"])
    def me():
        conn = _connect(); cur = conn.cursor()
        u = _session_user(cur); conn.close()
        return jsonify({"ok": True, "user": _public(u)}) if u else _err("Session expired. Sign in again.", 401)

    @route("/auth/logout", ["POST"])
    def logout():
        h = request.headers.get("Authorization", "")
        if h.startswith("Bearer "):
            conn = _connect(); cur = conn.cursor()
            cur.execute("DELETE FROM dbo.DASH_SESSIONS WHERE TokenHash = ?", _tok_hash(h[7:].strip()))
            conn.close()
        return jsonify({"ok": True})

    @route("/auth/change-password", ["POST"])
    def change_password():
        body = request.get_json(silent=True) or {}
        conn = _connect(); cur = conn.cursor()
        u = _session_user(cur)
        if not u:
            conn.close(); return _err("Session expired. Sign in again.", 401)
        row = _get_user(cur, u["id"])
        cur_pw, new_pw = str(body.get("current", "")), str(body.get("new", ""))
        if not check_pw(cur_pw, row[4]):
            conn.close(); return _err("Current password is incorrect.", 400)
        if len(new_pw) < MIN_PW:
            conn.close(); return _err(f"New password must be at least {MIN_PW} characters.", 400)
        if new_pw == cur_pw:
            conn.close(); return _err("New password must be different from the current one.", 400)
        cur.execute("UPDATE dbo.DASH_USERS SET PwHash = ?, MustChange = 0, UpdatedAt = SYSUTCDATETIME(), UpdatedBy = ? WHERE UserId = ?",
                    hash_pw(new_pw), u["id"], u["id"])
        conn.close()
        return jsonify({"ok": True})

    # ---- admin ----
    def _admin(cur):
        u = _session_user(cur)
        if not u:
            return None, _err("Session expired. Sign in again.", 401)
        if u["role"] != "admin":
            return None, _err("Only an admin can manage users.", 403)
        return u, None

    @route("/auth/users", ["GET", "POST"])
    def users():
        conn = _connect(); cur = conn.cursor()
        me_, bad = _admin(cur)
        if bad:
            conn.close(); return bad
        if request.method == "GET":
            cur.execute(f"SELECT {COLS} FROM dbo.DASH_USERS ORDER BY UserId")
            out = [_row_user(r) for r in cur.fetchall()]
            conn.close()
            return jsonify({"ok": True, "users": out, "roles": ROLES})
        b = request.get_json(silent=True) or {}
        uid = str(b.get("id", "")).strip().lower()
        name = str(b.get("displayName", "")).strip()
        role = str(b.get("role", "")).strip()
        pw = str(b.get("password", ""))
        if not uid or " " in uid:
            conn.close(); return _err("Enter a user ID without spaces.", 400)
        if not name:
            conn.close(); return _err("Enter a display name.", 400)
        if role not in ROLES:
            conn.close(); return _err("Pick a role.", 400)
        if len(pw) < MIN_PW:
            conn.close(); return _err(f"Password must be at least {MIN_PW} characters.", 400)
        try:
            acc = _clean_access(b.get("access", []))
        except ValueError as e:
            conn.close(); return _err(str(e), 400)
        if _get_user(cur, uid):
            conn.close(); return _err("That user ID already exists.", 409)
        cur.execute("INSERT INTO dbo.DASH_USERS (UserId, DisplayName, Role, Access, PwHash, MustChange, UpdatedBy) VALUES (?,?,?,?,?,?,?)",
                    uid, name, role, _acc_out(acc), hash_pw(pw), 1 if b.get("mustChange", True) else 0, me_["id"])
        row = _get_user(cur, uid); conn.close()
        _log(f"{me_['id']} created {uid}")
        return jsonify({"ok": True, "user": _row_user(row)})

    @route("/auth/users/<uid>", ["PUT"])
    def update_user(uid):
        conn = _connect(); cur = conn.cursor()
        me_, bad = _admin(cur)
        if bad:
            conn.close(); return bad
        row = _get_user(cur, uid)
        if not row:
            conn.close(); return _err("No such user.", 404)
        cur_u = _row_user(row)
        b = request.get_json(silent=True) or {}
        name = str(b.get("displayName", cur_u["displayName"])).strip() or cur_u["displayName"]
        role = str(b.get("role", cur_u["role"])).strip()
        active = bool(b.get("active", cur_u["active"]))
        if role not in ROLES:
            conn.close(); return _err("Pick a role.", 400)
        try:
            acc = _clean_access(b["access"]) if "access" in b else cur_u["access"]
        except ValueError as e:
            conn.close(); return _err(str(e), 400)
        losing_admin = cur_u["role"] == "admin" and cur_u["active"] and (role != "admin" or not active)
        if losing_admin and _admins_left(cur, cur_u["id"]) == 0:
            conn.close(); return _err("Keep at least one active admin.", 400)
        cur.execute("UPDATE dbo.DASH_USERS SET DisplayName = ?, Role = ?, Access = ?, Active = ?, UpdatedAt = SYSUTCDATETIME(), UpdatedBy = ? WHERE UserId = ?",
                    name, role, _acc_out(acc), 1 if active else 0, me_["id"], cur_u["id"])
        if not active:
            cur.execute("DELETE FROM dbo.DASH_SESSIONS WHERE UserId = ?", cur_u["id"])
        row = _get_user(cur, cur_u["id"]); conn.close()
        _log(f"{me_['id']} updated {cur_u['id']}")
        return jsonify({"ok": True, "user": _row_user(row)})

    @route("/auth/users/<uid>/reset-password", ["POST"])
    def reset_password(uid):
        conn = _connect(); cur = conn.cursor()
        me_, bad = _admin(cur)
        if bad:
            conn.close(); return bad
        row = _get_user(cur, uid)
        if not row:
            conn.close(); return _err("No such user.", 404)
        b = request.get_json(silent=True) or {}
        pw = str(b.get("password") or "")
        temp = None
        if not pw:
            pw = temp = gen_temp_password()
        if len(pw) < MIN_PW:
            conn.close(); return _err(f"Password must be at least {MIN_PW} characters.", 400)
        cur.execute("UPDATE dbo.DASH_USERS SET PwHash = ?, MustChange = ?, UpdatedAt = SYSUTCDATETIME(), UpdatedBy = ? WHERE UserId = ?",
                    hash_pw(pw), 1 if b.get("mustChange", True) else 0, me_["id"], row[0])
        cur.execute("DELETE FROM dbo.DASH_SESSIONS WHERE UserId = ?", row[0])   # sign them out everywhere
        conn.close()
        _log(f"{me_['id']} reset password for {row[0]}")
        return jsonify({"ok": True, "tempPassword": temp})

    _log("routes registered: /auth/login, /auth/me, /auth/logout, /auth/change-password, /auth/users")
