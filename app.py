import os
import json
import base64
import io
from datetime import datetime, date
from pathlib import Path
from functools import wraps

from flask import (Flask, request, jsonify, render_template,
                   session, redirect, url_for, send_file)
import anthropic
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.worksheet.datavalidation import DataValidation

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

# ── Env vars ──────────────────────────────────────────────────────────────────
DATABASE_URL   = os.environ.get("DATABASE_URL", "")          # Postgres on Railway
APP_PASSWORD   = os.environ.get("APP_PASSWORD", "")          # shared team password
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL          = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

CATEGORIES     = ["Recording","Mixing & Mastering","Music Video","Marketing",
                  "Sync/Licensing","Distribution","Legal","Merch","Tour/Live","Other"]
PAYMENT_METHODS = ["ACH","Check","Wire","Credit Card","Cash"]

EXTRACT_PROMPT = """Extract the following fields from this invoice or receipt.
Return ONLY a valid JSON object — no markdown, no extra text:

{
  "invoice_date": "MM/DD/YYYY if found, else empty string",
  "payee": "vendor or company name",
  "description": "brief description of what was invoiced (1 sentence max)",
  "category": "best match from: Recording, Mixing & Mastering, Music Video, Marketing, Sync/Licensing, Distribution, Legal, Merch, Tour/Live, Other",
  "invoice_number": "invoice number or reference if present, else empty string",
  "amount": <number with 2 decimal places, no currency symbols, 0 if not found>,
  "payment_method": "best match from: ACH, Check, Wire, Credit Card, Cash — or empty string if not shown"
}
"""


# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        url = DATABASE_URL
        # Railway sometimes uses postgres:// but psycopg2 needs postgresql://
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]
        conn = psycopg2.connect(url)
        return conn, "pg"
    else:
        import sqlite3
        db_path = Path(__file__).parent / "boom.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn, "sqlite"


def init_db():
    conn, kind = get_db()
    cur = conn.cursor()
    if kind == "pg":
        cur.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id            SERIAL PRIMARY KEY,
                invoice_date  DATE,
                payee         TEXT,
                description   TEXT,
                category      TEXT,
                artist        TEXT,
                invoice_number TEXT,
                amount        NUMERIC(12,2),
                payment_method TEXT,
                payment_date  DATE,
                in_quickbooks TEXT DEFAULT 'No',
                qb_entry_date DATE,
                uploaded_to_stem TEXT DEFAULT 'No',
                stem_upload_date DATE,
                notes         TEXT,
                created_at    TIMESTAMP DEFAULT NOW()
            )
        """)
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_date   TEXT,
                payee          TEXT,
                description    TEXT,
                category       TEXT,
                artist         TEXT,
                invoice_number TEXT,
                amount         REAL,
                payment_method TEXT,
                payment_date   TEXT,
                in_quickbooks  TEXT DEFAULT 'No',
                qb_entry_date  TEXT,
                uploaded_to_stem TEXT DEFAULT 'No',
                stem_upload_date TEXT,
                notes          TEXT,
                created_at     TEXT DEFAULT (datetime('now'))
            )
        """)
    conn.commit()
    conn.close()


# ── Auth ──────────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if APP_PASSWORD and not session.get("authenticated"):
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET","POST"])
def login():
    error = None
    if request.method == "POST":
        pw = request.form.get("password","")
        if pw == APP_PASSWORD:
            session["authenticated"] = True
            return redirect("/")
        error = "Incorrect password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


# ── Main routes ───────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return render_template("index.html",
                           categories=CATEGORIES,
                           payment_methods=PAYMENT_METHODS,
                           api_configured=bool(ANTHROPIC_KEY))


@app.route("/parse", methods=["POST"])
@login_required
def parse_invoice():
    api_key = ANTHROPIC_KEY
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY is not set on the server."}), 400

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded."}), 400

    file = request.files["file"]
    file_bytes = file.read()
    ext = Path(file.filename).suffix.lower()

    mime_map = {".pdf":"application/pdf", ".jpg":"image/jpeg",
                ".jpeg":"image/jpeg", ".png":"image/png", ".webp":"image/webp"}
    mime = mime_map.get(ext, file.content_type or "image/jpeg")

    b64 = base64.standard_b64encode(file_bytes).decode("utf-8")

    if mime == "application/pdf":
        content = [
            {"type":"document","source":{"type":"base64","media_type":"application/pdf","data":b64}},
            {"type":"text","text":EXTRACT_PROMPT}
        ]
    else:
        content = [
            {"type":"image","source":{"type":"base64","media_type":mime,"data":b64}},
            {"type":"text","text":EXTRACT_PROMPT}
        ]

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(model=MODEL, max_tokens=512,
                                      messages=[{"role":"user","content":content}])
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        extracted = json.loads(raw.strip())
    except json.JSONDecodeError as e:
        return jsonify({"error": f"Could not parse Claude response: {e}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    preview = f"data:{mime};base64,{b64}" if mime != "application/pdf" else None
    return jsonify({"fields": extracted, "preview": preview, "is_pdf": mime == "application/pdf"})


@app.route("/add", methods=["POST"])
@login_required
def add_expense():
    data = request.json

    def val(k, default=""):
        v = data.get(k, default)
        return v if v not in (None, "") else default

    def parse_date(s):
        if not s:
            return None
        for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y","%d/%m/%Y"):
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except ValueError:
                continue
        return None

    def parse_amount(v):
        try:
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).replace("$","").replace(",","").strip()
            return float(s) if s else None
        except ValueError:
            return None

    row = (
        parse_date(val("invoice_date")),
        val("payee"),
        val("description"),
        val("category"),
        val("artist"),
        val("invoice_number"),
        parse_amount(val("amount", 0)),
        val("payment_method"),
        parse_date(val("payment_date")),
        val("in_quickbooks","No"),
        parse_date(val("qb_entry_date")),
        val("uploaded_to_stem","No"),
        parse_date(val("stem_upload_date")),
        val("notes"),
    )

    try:
        conn, kind = get_db()
        cur = conn.cursor()
        ph = "%s" if kind == "pg" else "?"
        cur.execute(f"""
            INSERT INTO expenses
              (invoice_date,payee,description,category,artist,invoice_number,
               amount,payment_method,payment_date,in_quickbooks,qb_entry_date,
               uploaded_to_stem,stem_upload_date,notes)
            VALUES ({",".join([ph]*14)})
        """, row)
        if kind == "pg":
            cur.execute("SELECT lastval()")
            new_id = cur.fetchone()[0]
        else:
            new_id = cur.lastrowid
        conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"ok": True, "id": new_id, "payee": val("payee"), "amount": val("amount")})


@app.route("/recent")
@login_required
def recent():
    try:
        conn, kind = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT invoice_date,payee,amount,artist,in_quickbooks,uploaded_to_stem
            FROM expenses ORDER BY id DESC LIMIT 10
        """)
        rows = cur.fetchall()
        conn.close()
        result = []
        for r in rows:
            result.append({
                "date":   str(r[0] or ""),
                "payee":  str(r[1] or ""),
                "amount": r[2],
                "artist": str(r[3] or ""),
                "qb":     str(r[4] or ""),
                "stem":   str(r[5] or ""),
            })
        return jsonify(result)
    except Exception as e:
        return jsonify([])


@app.route("/export")
@login_required
def export_excel():
    """Generate and return a fresh Excel tracker from DB data."""
    try:
        conn, kind = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT invoice_date,payee,description,category,artist,invoice_number,
                   amount,payment_method,payment_date,in_quickbooks,qb_entry_date,
                   uploaded_to_stem,stem_upload_date,notes
            FROM expenses ORDER BY invoice_date ASC, id ASC
        """)
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    wb = _build_excel(rows)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    fname = f"BoomRecords_Expenses_{date.today().strftime('%Y-%m-%d')}.xlsx"
    return send_file(buf, as_attachment=True, download_name=fname,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def _build_excel(rows):
    DARK_NAVY = "FF1A1A2E"
    ACCENT    = "FF274472"
    WHITE     = "FFFFFFFF"
    LIGHT     = "FFF5F5F5"
    MID       = "FFD6D6D6"
    GREEN_HL  = "FFD9EAD3"
    YELLOW_HL = "FFFFF2CC"
    ORANGE_HL = "FFFCE5CD"
    BLUE_TEXT = "FF0000FF"

    def fill(c): return PatternFill("solid", start_color=c, end_color=c)
    def bdr():
        s = Side(style="thin", color="FFB0B0B0")
        return Border(left=s, right=s, top=s, bottom=s)

    wb = Workbook()
    ws = wb.active
    ws.title = "Expense Tracker"
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "A3"

    ws.merge_cells("A1:N1")
    ws["A1"] = "BOOM RECORDS — EXPENSE & RECOUPMENT TRACKER"
    ws["A1"].font = Font(name="Arial", bold=True, size=13, color=WHITE)
    ws["A1"].fill = fill(DARK_NAVY)
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 28

    hdrs = [("A","Invoice Date",14),("B","Payee / Vendor",22),("C","Description",30),
            ("D","Category",20),("E","Artist / Project",20),("F","Invoice #",14),
            ("G","Amount ($)",13),("H","Payment Method",16),("I","Payment Date",14),
            ("J","In QuickBooks?",16),("K","QB Entry Date",14),
            ("L","Uploaded to Stem?",18),("M","Stem Upload Date",16),("N","Notes",30)]

    for col, label, width in hdrs:
        c = ws[f"{col}2"]
        c.value = label
        c.font = Font(name="Arial", bold=True, size=10, color=WHITE)
        c.fill = fill(ACCENT)
        c.alignment = Alignment(horizontal="center", vertical="center")
        c.border = bdr()
        ws.column_dimensions[col].width = width
    ws.row_dimensions[2].height = 22

    DATE_FMT  = "MM/DD/YYYY"
    MONEY_FMT = '$#,##0.00;($#,##0.00);"-"'

    for i, row in enumerate(rows):
        r = i + 3
        rf = fill(LIGHT) if r % 2 == 0 else fill(WHITE)
        vals = list(row)

        for j, (col, _, _) in enumerate(hdrs):
            c = ws[f"{col}{r}"]
            c.fill = rf
            c.font = Font(name="Arial", size=10)
            c.border = bdr()
            c.alignment = Alignment(horizontal="left", vertical="center")

        # Set values
        ws[f"A{r}"] = vals[0]; ws[f"A{r}"].number_format = DATE_FMT; ws[f"A{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"B{r}"] = vals[1]
        ws[f"C{r}"] = vals[2]
        ws[f"D{r}"] = vals[3]; ws[f"D{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"E{r}"] = vals[4]
        ws[f"F{r}"] = vals[5]; ws[f"F{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"G{r}"] = vals[6]; ws[f"G{r}"].number_format = MONEY_FMT; ws[f"G{r}"].alignment = Alignment(horizontal="right",vertical="center")
        ws[f"H{r}"] = vals[7]; ws[f"H{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"I{r}"] = vals[8]; ws[f"I{r}"].number_format = DATE_FMT; ws[f"I{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"J{r}"] = vals[9]; ws[f"J{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"K{r}"] = vals[10]; ws[f"K{r}"].number_format = DATE_FMT; ws[f"K{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"L{r}"] = vals[11]; ws[f"L{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"M{r}"] = vals[12]; ws[f"M{r}"].number_format = DATE_FMT; ws[f"M{r}"].alignment = Alignment(horizontal="center",vertical="center")
        ws[f"N{r}"] = vals[13]

        # Row color override based on status
        qb   = str(vals[9] or "")
        stem = str(vals[11] or "")
        if qb == "Yes" and stem == "Yes":
            row_fill = fill(GREEN_HL)
        elif qb == "No":
            row_fill = fill(YELLOW_HL)
        elif stem == "No":
            row_fill = fill(ORANGE_HL)
        else:
            row_fill = rf

        if row_fill != rf:
            for col, _, _ in hdrs:
                ws[f"{col}{r}"].fill = row_fill

    return wb


# ── Status endpoint ───────────────────────────────────────────────────────────

@app.route("/ledger")
@login_required
def ledger():
    return render_template("ledger.html")


@app.route("/entries")
@login_required
def entries():
    try:
        conn, kind = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, invoice_date, payee, description, category, artist,
                   invoice_number, amount, payment_method, payment_date,
                   in_quickbooks, uploaded_to_stem, notes
            FROM expenses ORDER BY invoice_date DESC, id DESC
        """)
        rows = cur.fetchall()
        conn.close()
        result = []
        for r in rows:
            result.append({
                "id":             r[0],
                "invoice_date":   str(r[1] or ""),
                "payee":          str(r[2] or ""),
                "description":    str(r[3] or ""),
                "category":       str(r[4] or ""),
                "artist":         str(r[5] or ""),
                "invoice_number": str(r[6] or ""),
                "amount":         r[7],
                "payment_method": str(r[8] or ""),
                "payment_date":   str(r[9] or ""),
                "in_quickbooks":  str(r[10] or ""),
                "uploaded_to_stem": str(r[11] or ""),
                "notes":          str(r[12] or ""),
            })
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/delete/<int:entry_id>", methods=["POST"])
@login_required
def delete_entry(entry_id):
    try:
        conn, kind = get_db()
        cur = conn.cursor()
        ph = "%s" if kind == "pg" else "?"
        cur.execute(f"DELETE FROM expenses WHERE id = {ph}", (entry_id,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/status")
def status():
    return jsonify({"ok": True})


# ── Boot ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5100))
    print(f"\n  Boom Records Invoice Parser  →  http://localhost:{port}\n")
    app.run(debug=False, host="0.0.0.0", port=port)
