# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
pip install -r requirements.txt
python app.py                # Dev server on http://localhost:5100
```

Production (Railway/Heroku):
```bash
gunicorn wsgi:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120
```

No test suite exists. Testing is manual via the app and John's `/impersonate/<username>` route.

## Architecture

**Single-file Flask app** (`app.py`, ~3300 lines) serving Jinja2 templates. No frontend build step.

### Database
- PostgreSQL in production (`DATABASE_URL` env var), SQLite fallback (`boom.db`) for local dev.
- Raw SQL via `psycopg2`/`sqlite3` — no ORM.
- `init_db()` runs on import: creates tables and applies migrations inline.
- Three tables: `expenses` (core ledger, 40+ columns), `audit_log` (change tracking), `app_users` (user management).
- File attachments (invoices, W-9s, proof of payment) stored as base64 blobs in the `expenses` table.
- Soft deletes via `deleted` flag; all mutations logged to `audit_log`.

### Auth & Roles
- **Google OAuth** (Authlib) is the primary auth mechanism. Fallback password auth for local dev.
- User whitelist: `GOOGLE_ALLOWED_EMAILS` dict in `app.py` (~line 58) maps emails to (name, role).
- Role hierarchy: `superadmin > admin > manager > user` (numeric `ROLE_LEVEL` dict).
- Decorators: `@login_required`, `@page_required("page_key")`, `@admin_required`.
- Per-user page permissions stored as JSON array in `app_users.allowed_pages`.

### Claude AI Integration
- `extract_fields()`: Vision API parses uploaded invoices/receipts into structured fields.
- `_validate_file()`: Pre-submission validation of invoice completeness and W-9 correctness.
- Model configured via `CLAUDE_MODEL` env var (default: `claude-sonnet-4-6`).

### Key Route Groups
- **Invoice CRUD**: `/add`, `/update/<eid>`, `/delete/<eid>`, `/restore/<eid>`, `/parse`
- **Approvals**: `/approvals`, `/approve/<eid>`, `/approve-bulk`, `/reject/<eid>`
- **File management**: `/add-invoice/<eid>`, `/add-w9/<eid>`, `/add-proof/<eid>`, `/invoice/<eid>`, `/w9/<eid>`, `/proof/<eid>`
- **Reporting**: `/ledger`, `/analytics`, `/1099`, `/calendar`, `/history`, `/payments`
- **Export**: `/export` (Excel), `/export-qbo` (QuickBooks CSV), `/admin/backup` (full dump)
- **Public vendor portal**: `/submit` (no auth required)
- **API for React dashboard** (`boom-combined`): `/api/dashboard-summary`, `/api/pending-count`
- **Admin**: `/settings` (user CRUD, page permissions)

### Constants (in app.py)
- `CATEGORIES`: 10 expense categories (Recording, Mixing, Music Video, etc.)
- `PAYMENT_METHODS`: 6 methods (ACH, Check, Wire, Credit Card, PayPal, Cash)
- `ALL_PAGES`: 11 dashboard pages with default role-based visibility.
- `MAX_CONTENT_LENGTH`: 25MB. Allowed extensions: pdf, jpg, jpeg, png.

## Environment Variables

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection (omit for SQLite) |
| `ANTHROPIC_API_KEY` | Claude Vision for invoice parsing |
| `CLAUDE_MODEL` | Model override (default: claude-sonnet-4-6) |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | OAuth |
| `SECRET_KEY` | Flask sessions (auto-generated if unset) |
| `GMAIL_CLIENT_ID` / `GMAIL_CLIENT_SECRET` / `GMAIL_REFRESH_TOKEN` | Email via Gmail OAuth |
| `GMAIL_USER` | Sender email address |
| `NOTIFY_EMAIL` | Comma-separated notification recipients |
| `APP_URL` | Full app URL for emails/redirects (default: https://boominvoiceapp.up.railway.app) |
| `PORT` | Server port (default: 5100) |
| `BACKUP_TOKEN` | Secret for `/backup/auto` cron endpoint |

## DB Helper Pattern

All queries use a `get_db()` context manager that returns `(conn, cur)` and handles commit/close. Queries use `%s` placeholders for PostgreSQL and `?` for SQLite — the helper abstracts this via `ph()` which returns the correct placeholder string.
