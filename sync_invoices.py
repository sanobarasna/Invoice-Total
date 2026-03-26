# ==========================================================
# sync_invoices.py
#
# Reads the "Invoices" sheet from the Excel workbook
# "INVOICE ENTRY MACRO ENABLED.xlsm" (or .xlsx) and
# upserts every row into the Supabase `invoices` table.
#
# Columns synced (by position, not header name, so layout
# changes don't silently break the sync):
#   A  → date
#   B  → supplier
#   W  → invoice_total
#   Y  → total_sale
#
# Run manually:          python sync_invoices.py
# Run on a schedule:     add to cron / Task Scheduler
# Run as file watcher:   combine with watchdog (see bottom)
#
# Secrets — same .streamlit/secrets.toml used by the dashboard:
#   SUPABASE_URL = "https://xxxx.supabase.co"
#   SUPABASE_KEY = "your-anon-public-key"
#   EXCEL_PATH   = "C:/path/to/INVOICE ENTRY MACRO ENABLED.xlsm"
# ==========================================================

import os
import sys
import math
from datetime import datetime, date
from pathlib import Path

import toml
import pandas as pd
from supabase import create_client

# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------
SHEET_NAME  = "Invoices"
TABLE_NAME  = "invoices"

# Column letters → zero-based index
COL_DATE          = 0   # A
COL_SUPPLIER      = 1   # B
COL_INVOICE_TOTAL = 22  # W
COL_TOTAL_SALE    = 24  # Y

# How many rows to upsert per batch (Supabase limit)
BATCH_SIZE = 500


# ----------------------------------------------------------
# LOAD SECRETS
# ----------------------------------------------------------
def load_secrets() -> dict:
    """Read from .streamlit/secrets.toml relative to this script."""
    secrets_path = Path(__file__).parent / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        # Fall back to environment variables
        return {
            "SUPABASE_URL": os.environ.get("SUPABASE_URL"),
            "SUPABASE_KEY": os.environ.get("SUPABASE_KEY"),
            "EXCEL_PATH":   os.environ.get("EXCEL_PATH"),
        }
    return toml.load(secrets_path)


# ----------------------------------------------------------
# EXCEL → DATAFRAME
# ----------------------------------------------------------
def read_invoice_sheet(excel_path: str) -> pd.DataFrame:
    """
    Read only the four columns we need from the Invoices sheet.
    Uses openpyxl engine so .xlsm macro-enabled files open without errors.
    Skips blank rows and the header row.
    """
    print(f"📂  Reading: {excel_path}  →  sheet='{SHEET_NAME}'")

    df_raw = pd.read_excel(
        excel_path,
        sheet_name=SHEET_NAME,
        engine="openpyxl",
        header=0,          # row 1 is the header
        dtype=str,         # read everything as text first; we'll coerce below
    )

    # Grab columns by position (A=0, B=1, W=22, Y=24)
    needed = {
        "date":          df_raw.columns[COL_DATE],
        "supplier":      df_raw.columns[COL_SUPPLIER],
        "invoice_total": df_raw.columns[COL_INVOICE_TOTAL],
        "total_sale":    df_raw.columns[COL_TOTAL_SALE],
    }

    df = df_raw[[needed["date"], needed["supplier"],
                 needed["invoice_total"], needed["total_sale"]]].copy()
    df.columns = ["date", "supplier", "invoice_total", "total_sale"]

    # Drop rows where both supplier AND date are blank
    df = df[~(df["supplier"].isna() & df["date"].isna())]
    df = df[df["supplier"].notna() & (df["supplier"].str.strip() != "")]

    # Normalise date → ISO string "YYYY-MM-DD"
    def parse_date(val):
        if pd.isna(val) or str(val).strip() in ("", "nan", "None"):
            return None
        try:
            # Excel sometimes gives a float serial number or a datetime string
            parsed = pd.to_datetime(val, dayfirst=False, errors="coerce")
            if pd.isna(parsed):
                return None
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            return None

    df["date"] = df["date"].apply(parse_date)
    df = df[df["date"].notna()]   # drop rows with unparseable dates

    # Numeric columns
    for col in ["invoice_total", "total_sale"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[,$\s]", "", regex=True)   # strip currency symbols
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )

    # Clean supplier name
    df["supplier"] = df["supplier"].astype(str).str.strip().str.upper()

    print(f"✅  Parsed {len(df):,} valid invoice rows")
    return df.reset_index(drop=True)


# ----------------------------------------------------------
# UPSERT TO SUPABASE
# ----------------------------------------------------------
def upsert_to_supabase(df: pd.DataFrame, url: str, key: str) -> None:
    """
    Delete all existing rows then re-insert.
    (Full refresh keeps things simple and consistent.)
    If your table grows very large, switch to upsert with a unique key.
    """
    client = create_client(url, key)

    print(f"🗑️   Clearing existing rows from '{TABLE_NAME}'…")
    # Delete in chunks to avoid URI-too-long errors on large tables
    client.table(TABLE_NAME).delete().neq("date", "1900-01-01").execute()

    records = df.to_dict(orient="records")
    total   = len(records)
    batches = math.ceil(total / BATCH_SIZE)

    print(f"⬆️   Upserting {total:,} rows in {batches} batch(es)…")
    for i in range(batches):
        batch = records[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
        client.table(TABLE_NAME).insert(batch).execute()
        print(f"     Batch {i+1}/{batches} — {len(batch)} rows inserted")

    print(f"🎉  Sync complete — {total:,} rows in '{TABLE_NAME}'")


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
def run_sync(excel_path: str = None):
    secrets = load_secrets()

    url  = secrets.get("SUPABASE_URL")
    key  = secrets.get("SUPABASE_KEY")
    path = excel_path or secrets.get("EXCEL_PATH")

    if not url or not key:
        print("❌  SUPABASE_URL and SUPABASE_KEY must be set in secrets.toml or env vars.")
        sys.exit(1)
    if not path:
        print("❌  EXCEL_PATH must be set in secrets.toml or passed as argument.")
        sys.exit(1)
    if not Path(path).exists():
        print(f"❌  File not found: {path}")
        sys.exit(1)

    df = read_invoice_sheet(path)
    upsert_to_supabase(df, url, key)


# ----------------------------------------------------------
# OPTIONAL FILE WATCHER  (pip install watchdog)
# Monitors the Excel file and re-syncs on every save.
# ----------------------------------------------------------
def watch_and_sync(excel_path: str):
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
        import time
    except ImportError:
        print("⚠️  watchdog not installed — run:  pip install watchdog")
        sys.exit(1)

    class ExcelHandler(FileSystemEventHandler):
        def on_modified(self, event):
            if Path(event.src_path).resolve() == Path(excel_path).resolve():
                print(f"\n🔔  Change detected at {datetime.now():%H:%M:%S} — re-syncing…")
                try:
                    run_sync(excel_path)
                except Exception as exc:
                    print(f"❌  Sync failed: {exc}")

    observer = Observer()
    observer.schedule(ExcelHandler(), path=str(Path(excel_path).parent), recursive=False)
    observer.start()
    print(f"👁️   Watching: {excel_path}")
    print("     Press Ctrl+C to stop.\n")
    try:
        while True:
            import time; time.sleep(2)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# ----------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sync Invoices sheet → Supabase")
    parser.add_argument("--path",  help="Path to Excel workbook (overrides secrets.toml)")
    parser.add_argument("--watch", action="store_true", help="Watch file for changes and auto-sync")
    args = parser.parse_args()

    if args.watch:
        secrets  = load_secrets()
        xl_path  = args.path or secrets.get("EXCEL_PATH")
        watch_and_sync(xl_path)
    else:
        run_sync(args.path)
