# ==========================================================
# invoice_dashboard.py — Invoice Totals Dashboard
#
# Reads from the `invoices` Supabase table (synced by
# sync_invoices.py) and displays invoice / sale totals
# grouped by supplier and date.
#
# Secrets required in .streamlit/secrets.toml:
#   SUPABASE_URL = "https://xxxx.supabase.co"
#   SUPABASE_KEY = "your-anon-public-key"
# ==========================================================

import io
from datetime import datetime, date, timedelta
from pathlib import Path

import pytz
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Invoice Dashboard", layout="wide")

# ----------------------------------------------------------
# CSS  (reuse styles.css if it exists alongside this file)
# ----------------------------------------------------------
def load_css():
    css_file = Path(__file__).parent / "styles.css"
    if css_file.exists():
        with open(css_file) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()
st.title("🧾 Invoice Dashboard")

# ==========================================================
# SUPABASE CONNECTION
# ==========================================================
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return default

SUPABASE_URL = get_secret("SUPABASE_URL")
SUPABASE_KEY = get_secret("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error(
        "❌ Supabase secrets not found. "
        "Add **SUPABASE_URL** and **SUPABASE_KEY** to your Streamlit secrets."
    )
    st.stop()

@st.cache_resource
def get_supabase_client():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ==========================================================
# PAGINATED FETCH
# ==========================================================
def fetch_all(table: str, columns: str = "*") -> list[dict]:
    client   = get_supabase_client()
    page     = 0
    size     = 1000
    all_rows = []
    while True:
        result = (
            client.table(table)
            .select(columns)
            .range(page * size, (page + 1) * size - 1)
            .execute()
        )
        batch = result.data or []
        all_rows.extend(batch)
        if len(batch) < size:
            break
        page += 1
    return all_rows


# ==========================================================
# DATA LOADER
# ==========================================================
@st.cache_data(ttl=300)
def load_invoices() -> pd.DataFrame:
    rows = fetch_all("invoices", "date, supplier, invoice_total, total_sale")
    df   = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["date", "supplier", "invoice_total", "total_sale"])

    df["date"]          = pd.to_datetime(df["date"], errors="coerce")
    df["invoice_total"] = pd.to_numeric(df["invoice_total"], errors="coerce").fillna(0)
    df["total_sale"]    = pd.to_numeric(df["total_sale"],    errors="coerce").fillna(0)
    df["supplier"]      = df["supplier"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["date"])
    return df.sort_values("date").reset_index(drop=True)


# ==========================================================
# REFRESH CONTROL
# ==========================================================
st.markdown("---")
col_refresh, col_ts, _ = st.columns([1.5, 3, 5])

with col_refresh:
    if st.button("🔄 Refresh Data", use_container_width=True):
        load_invoices.clear()
        st.rerun()

cst     = pytz.timezone("America/Chicago")
now_cst = datetime.now(cst).strftime("%Y-%m-%d %I:%M %p CST")
with col_ts:
    st.caption(f"🕐 Data loaded at {now_cst} — auto-refreshes every 5 minutes")

st.markdown("---")

# ==========================================================
# LOAD DATA
# ==========================================================
df_raw = load_invoices()

if df_raw.empty:
    st.error(
        "❌ No data found in the `invoices` table. "
        "Run **sync_invoices.py** to populate it from your Excel workbook."
    )
    st.stop()

# ==========================================================
# FILTERS
# ==========================================================
min_date = df_raw["date"].min().date()
max_date = df_raw["date"].max().date()

# Default to current month
today        = date.today()
default_start = today.replace(day=1)
default_end   = today

if "inv_clear" not in st.session_state:
    st.session_state.inv_clear = 0

fc1, fc2, fc3, fc4 = st.columns([2, 2, 3, 1])

with fc1:
    date_start = st.date_input(
        "From",
        value=max(default_start, min_date),
        min_value=min_date,
        max_value=max_date,
        key=f"inv_start_{st.session_state.inv_clear}",
    )
with fc2:
    date_end = st.date_input(
        "To",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        key=f"inv_end_{st.session_state.inv_clear}",
    )
with fc3:
    all_suppliers = sorted(df_raw["supplier"].dropna().unique())
    sel_suppliers = st.multiselect(
        "Supplier",
        options=all_suppliers,
        default=[],
        placeholder="All suppliers",
        key=f"inv_sup_{st.session_state.inv_clear}",
    )
with fc4:
    st.markdown("<div style='padding-top:28px'>", unsafe_allow_html=True)
    if st.button("🔄 Clear", type="secondary", use_container_width=True, key="inv_clear_btn"):
        st.session_state.inv_clear += 1
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

# Apply filters
df = df_raw.copy()
df = df[(df["date"].dt.date >= date_start) & (df["date"].dt.date <= date_end)]
if sel_suppliers:
    df = df[df["supplier"].isin(sel_suppliers)]

if df.empty:
    st.warning("No invoice data matches the selected filters.")
    st.stop()

# ==========================================================
# KPI METRICS
# ==========================================================
total_invoice = df["invoice_total"].sum()
total_sale    = df["total_sale"].sum()
total_profit  = total_sale - total_invoice
profit_pct    = (total_profit / total_sale * 100) if total_sale else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("🧾 Invoice Rows",       f"{len(df):,}")
k2.metric("💰 Total Invoice Cost", f"${total_invoice:,.2f}")
k3.metric("🏷️ Total Sale Value",   f"${total_sale:,.2f}")
k4.metric("📈 Total Profit",       f"${total_profit:,.2f}",
          delta=f"{profit_pct:.1f}% margin")

st.markdown("---")

# ==========================================================
# DATE GROUPING TOGGLE
# ==========================================================
st.markdown("### 📅 Grouped Totals")

gcol1, gcol2, _ = st.columns([3, 5, 2])
with gcol1:
    st.markdown("<div style='padding-top:4px; font-size:14px; color:#555'>Group by</div>",
                unsafe_allow_html=True)
    g1, g2, g3 = st.columns(3)

    if "inv_group" not in st.session_state:
        st.session_state.inv_group = "Daily"

    with g1:
        if st.button("📆 Daily",
                     type="primary" if st.session_state.inv_group == "Daily" else "secondary",
                     use_container_width=True, key="grp_daily"):
            st.session_state.inv_group = "Daily"; st.rerun()
    with g2:
        if st.button("📅 Weekly",
                     type="primary" if st.session_state.inv_group == "Weekly" else "secondary",
                     use_container_width=True, key="grp_weekly"):
            st.session_state.inv_group = "Weekly"; st.rerun()
    with g3:
        if st.button("🗓️ Monthly",
                     type="primary" if st.session_state.inv_group == "Monthly" else "secondary",
                     use_container_width=True, key="grp_monthly"):
            st.session_state.inv_group = "Monthly"; st.rerun()

group_mode = st.session_state.inv_group

# ----------------------------------------------------------
# Build period label
# ----------------------------------------------------------
df_grp = df.copy()

if group_mode == "Daily":
    df_grp["Period"] = df_grp["date"].dt.strftime("%Y-%m-%d")
elif group_mode == "Weekly":
    # Label = Monday of the week
    df_grp["Period"] = (
        df_grp["date"] - pd.to_timedelta(df_grp["date"].dt.weekday, unit="D")
    ).dt.strftime("Week of %b %d, %Y")
else:  # Monthly
    df_grp["Period"] = df_grp["date"].dt.strftime("%B %Y")

# ==========================================================
# MAIN TABLE — Supplier × Period
# ==========================================================
main_grp = (
    df_grp.groupby(["supplier", "Period"])
    .agg(
        Entries        = ("invoice_total", "count"),
        Invoice_Total  = ("invoice_total", "sum"),
        Sale_Total     = ("total_sale",    "sum"),
    )
    .reset_index()
)
main_grp["Profit"]    = main_grp["Sale_Total"]   - main_grp["Invoice_Total"]
main_grp["Profit %"]  = main_grp.apply(
    lambda r: f"{r['Profit'] / r['Sale_Total'] * 100:.1f}%" if r["Sale_Total"] else "—", axis=1
)
main_grp = main_grp.rename(columns={
    "supplier":      "Supplier",
    "Period":        "Period",
    "Entries":       "# Entries",
    "Invoice_Total": "Invoice Total ($)",
    "Sale_Total":    "Sale Total ($)",
    "Profit":        "Profit ($)",
})
main_grp = main_grp.sort_values(["Supplier", "Period"]).reset_index(drop=True)
main_grp.index += 1

st.info(
    f"Showing **{len(main_grp):,}** rows across **{main_grp['Supplier'].nunique()}** supplier(s) "
    f"— grouped **{group_mode}**"
)

st.dataframe(
    main_grp,
    use_container_width=True,
    hide_index=False,
    height=460,
    column_config={
        "Invoice Total ($)": st.column_config.NumberColumn(format="$%.2f"),
        "Sale Total ($)":    st.column_config.NumberColumn(format="$%.2f"),
        "Profit ($)":        st.column_config.NumberColumn(format="$%.2f"),
    },
)

dl1, _, dl2, _ = st.columns([2, 0.3, 2, 5])
with dl1:
    st.download_button(
        "📥 Download Grouped Table (.csv)",
        data=main_grp.to_csv(index=False),
        file_name=f"invoices_{group_mode.lower()}_{date_start}_{date_end}.csv",
        mime="text/csv",
        type="secondary",
        use_container_width=True,
    )

st.markdown("---")

# ==========================================================
# SUPPLIER SUMMARY TABLE
# ==========================================================
st.markdown("### 🏭 Supplier Summary")
st.caption(f"Aggregated across {date_start} → {date_end}")

sup_grp = (
    df.groupby("supplier")
    .agg(
        Entries       = ("invoice_total", "count"),
        Invoice_Total = ("invoice_total", "sum"),
        Sale_Total    = ("total_sale",    "sum"),
    )
    .reset_index()
)
sup_grp["Profit"]   = sup_grp["Sale_Total"]   - sup_grp["Invoice_Total"]
sup_grp["Profit %"] = sup_grp.apply(
    lambda r: f"{r['Profit'] / r['Sale_Total'] * 100:.1f}%" if r["Sale_Total"] else "—", axis=1
)
sup_grp["Invoice Share %"] = sup_grp.apply(
    lambda r: f"{r['Invoice_Total'] / total_invoice * 100:.1f}%" if total_invoice else "—", axis=1
)
sup_grp = sup_grp.rename(columns={
    "supplier":      "Supplier",
    "Entries":       "# Entries",
    "Invoice_Total": "Invoice Total ($)",
    "Sale_Total":    "Sale Total ($)",
    "Profit":        "Profit ($)",
})
sup_grp = sup_grp.sort_values("Invoice Total ($)", ascending=False).reset_index(drop=True)
sup_grp.index += 1

st.dataframe(
    sup_grp,
    use_container_width=True,
    hide_index=False,
    height=min(50 + len(sup_grp) * 38, 460),
    column_config={
        "Invoice Total ($)": st.column_config.NumberColumn(format="$%.2f"),
        "Sale Total ($)":    st.column_config.NumberColumn(format="$%.2f"),
        "Profit ($)":        st.column_config.NumberColumn(format="$%.2f"),
    },
)

with dl2:
    st.download_button(
        "📥 Download Supplier Summary (.csv)",
        data=sup_grp.to_csv(index=False),
        file_name=f"supplier_summary_{date_start}_{date_end}.csv",
        mime="text/csv",
        type="secondary",
        use_container_width=True,
    )
