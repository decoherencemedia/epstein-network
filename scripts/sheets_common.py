from __future__ import annotations

from pathlib import Path

import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SCRIPT_DIR = Path(__file__).resolve().parent

# Google Sheets auth configuration shared by scripts.
CREDENTIALS_PATH = Path.home() / ".config" / "google-sheets-api" / "credentials.json"
TOKEN_PATH = Path.home() / ".config" / "google-sheets-api" / "token.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_google_credentials() -> Credentials:
    creds: Credentials | None = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_PATH.write_text(creds.to_json())
    return creds


def get_sheet_client() -> gspread.Client:
    """Return an authorized gspread client."""
    return gspread.authorize(get_google_credentials())


# Shared spreadsheet (Matches / Ignore sheets live here).
SPREADSHEET_ID_OR_TITLE = "1V4yiUKjhLq3r32_izswHUycMFvzCKUhhoTza6olORwo"


def get_workbook(
    gc: gspread.Client,
    id_or_title: str | None = None,
) -> gspread.Spreadsheet:
    """Open the workbook by ID or title. Creates it if not found (title only)."""
    key = id_or_title or SPREADSHEET_ID_OR_TITLE
    if key.startswith("1") and len(key) > 20 and all(c.isalnum() or c in "-_" for c in key):
        return gc.open_by_key(key)
    try:
        return gc.open(key)
    except gspread.SpreadsheetNotFound:
        return gc.create(key)


def load_names(gc: gspread.Client) -> dict[str, str]:
    """Load person_id -> name from the 'Matches' sheet (col A = Name, col B = Person ID)."""
    book = get_workbook(gc)
    ws = book.worksheet("Matches")
    rows = ws.get_all_values()
    if not rows:
        return {}
    # Assume row 0 is header; skip if first cell looks like "Name"
    start = 0
    if rows and rows[0] and rows[0][0].strip().lower() == "name":
        start = 1
    result: dict[str, str] = {}
    for row in rows[start:]:
        if len(row) >= 2:
            name = (row[0] or "").strip()
            person_id = (row[1] or "").strip()
            if person_id:
                result[person_id] = name
    return result


def load_ignore(gc: gspread.Client) -> set[str]:
    """Load set of person_ids from the 'Ignore' sheet (col A = Person ID)."""
    book = get_workbook(gc)
    ws = book.worksheet("Ignore")
    rows = ws.get_all_values()
    if not rows:
        return set()
    # Skip header if first cell looks like "Person ID"
    start = 0
    if rows and rows[0] and "person" in (rows[0][0] or "").strip().lower():
        start = 1
    return {(row[0] or "").strip() for row in rows[start:] if (row and (row[0] or "").strip())}


def load_categories(gc: gspread.Client) -> dict[str, str]:
    """Load name -> category from the 'Matches' sheet (col A = Name, col H = Category)."""
    book = get_workbook(gc)
    ws = book.worksheet("Matches")
    rows = ws.get_all_values()
    if not rows:
        return {}
    start = 0
    if rows and rows[0] and rows[0][0].strip().lower() == "name":
        start = 1
    result: dict[str, str] = {}
    for row in rows[start:]:
        if len(row) >= 8:
            name = (row[0] or "").strip()
            category = (row[7] or "").strip()  # column H = index 7
            if not name:
                continue
            # Names like "person_123" are considered unknown by convention.
            if name.startswith("person_") and not category:
                category = "unknown"
            result[name] = category
    return result


def _load_person_ids_from_col_b(book: gspread.Spreadsheet, sheet_name: str) -> set[str]:
    """Load person_ids from column B of the given sheet (skips header if it looks like one)."""
    ws = book.worksheet(sheet_name)
    rows = ws.get_all_values()
    if not rows:
        return set()
    start = 0
    # If header row contains "person" in col B, skip it.
    if len(rows[0]) >= 2 and "person" in (rows[0][1] or "").strip().lower():
        start = 1
    result: set[str] = set()
    for row in rows[start:]:
        if len(row) >= 2:
            pid = (row[1] or "").strip()
            if pid:
                result.add(pid)
    return result


def load_person_ids_matches_and_unknowns(gc: gspread.Client) -> set[str]:
    """
    Load person_ids to include in graphs.

    Source:
      - 'Matches' sheet, column B (Person ID)
      - 'Unknowns' sheet, column B (Person ID)
    """
    book = get_workbook(gc)
    ids = _load_person_ids_from_col_b(book, "Matches")
    ids |= _load_person_ids_from_col_b(book, "Unknowns")
    return ids
