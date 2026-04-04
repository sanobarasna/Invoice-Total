"""
invoice_dashboard.py

Invoice totals dashboard for Streamlit.

Highlights:
- Clear separation of UI, data loading, filtering, and aggregation
- Reusable helper functions
- Centralized constants
- Safer numeric/date normalization
- Easier to extend with more summary views
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Final

import httpx
import pandas as pd
import pytz
import streamlit as st

# ==========================================================
# APP CONFIG
# ==========================================================
APP_TITLE: Final[str] = "🧾 Invoice Dashboard"
PAGE_TITLE: Final[str] = "Invoice Dashboard"
TABLE_NAME: Final[str] = "invoices"
CACHE_TTL_SECONDS: Final[int] = 300
PAGE_SIZE: Final[int] = 1000
TIMEZONE_NAME: Final[str] = "America/Chicago"


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    key: str


# ==========================================================
# PAGE SETUP
# ==========================================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.title(APP_TITLE)


# ==========================================================
# STYLING
# ==========================================================
INLINE_CSS = """
<style>
html, body, [class*="css"] {
    font-size: 16px !important;
}

.stDataFrame iframe {
    font-size: 15px !important;
}
[data-testid="stDataFrame"] td,
[data-testid="stDataFrame"] th {
    font-size: 15px !important;
    padding: 10px 14px !important;
    line-height: 1.5 !important;
}

[data-testid="stMetricLabel"] {
    font-size: 15px !important;
}
[data-testid="stMetricValue"] {
    font-size: 28px !important;
}
[data-testid="stMetricDelta"] {
    font-size: 14px !important;
}

[data-testid="stCaptionContainer"],
.stAlert p {
    font-size: 14px !important;
}

label[data-testid="stWidgetLabel"] {
    font-size: 15px !important;
    font-weight: 600 !important;
}

[data-testid="stMultiSelect"] span,
[data-testid="stSelectbox"] div {
    font-size: 15px !important;
}

[data-testid="stButton"] button {
    font-size: 15px !important;
    padding: 8px 16px !important;
}

[data-testid="stInfo"] {
    font-size: 15px !important;
}
</style>
"""


def load_local_css() -> None:
    """Load styles.css from the same directory if present."""
    css_path = Path(__file__).parent / "styles.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def load_inline_css() -> None:
    """Apply inline CSS overrides."""
    st.markdown(INLINE_CSS, unsafe_allow_html=True)


load_local_css()
load_inline_css()


# ==========================================================
# CONFIG / SECRETS
# ==========================================================
def get_secret(key: str, default: str | None = None) -> str | None:
    try:
        return st.secrets[key]
    except Exception:
        return default


def get_supabase_config() -> SupabaseConfig:
    url = get_secret("SUPABASE_URL")
    key = get_secret("SUPABASE_KEY")

    if not url or not key:
        st.error(
            "❌ Supabase secrets not found. "
            "Add **SUPABASE_URL** and **SUPABASE_KEY** to your Streamlit secrets."
        )
        st.stop()

    return SupabaseConfig(url=url, key=key)


# ==========================================================
# DATA ACCESS
# ==========================================================
def fetch_all_rows(config: SupabaseConfig, table: str, columns: str = "*") -> list[dict]:
    """
    Fetch all rows from a Supabase REST table using pagination.
    """
    headers = {
        "apikey": config.key,
        "Authorization": f"Bearer {config.key}",
        "Accept": "application/json",
        "Range-Unit": "items",
        "Prefer": "count=none",
    }

    all_rows: list[dict] = []
    page = 0

    while True:
        start = page * PAGE_SIZE
        end = start + PAGE_SIZE - 1

        response = httpx.get(
            f"{config.url}/rest/v1/{table}",
            params={"select": columns},
            headers={**headers, "Range": f"{start}-{end}"},
            timeout=30,
        )
        response.raise_for_status()

        batch = response.json()
        all_rows.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        page += 1

    return all_rows


def normalize_invoice_dataframe(rows: list[dict]) -> pd.DataFrame:
    """
    Normalize raw Supabase records into a clean invoice DataFrame.
    """
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["date", "supplier", "invoice_total", "total_sale"])

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["invoice_total"] = pd.to_numeric(df["invoice_total"], errors="coerce").fillna(0.0)
    df["total_sale"] = pd.to_numeric(df["total_sale"], errors="coerce").fillna(0.0)
    df["supplier"] = df["supplier"].astype(str).str.strip().str.upper()

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_invoices_data(config: SupabaseConfig) -> pd.DataFrame:
    """
    Load and cache invoice data from Supabase.
    """
    rows = fetch_all_rows(
        config=config,
        table=TABLE_NAME,
        columns="date,supplier,invoice_total,total_sale",
    )
    return normalize_invoice_dataframe(rows)


# ==========================================================
# REFRESH
# ==========================================================
def render_refresh_controls() -> None:
    st.markdown("---")
    col_refresh, col_ts, _ = st.columns([1.5, 3, 5])

    with col_refresh:
        if st.button("🔄 Refresh Data", use_container_width=True):
            load_invoices_data.clear()
            st.rerun()

    timezone = pytz.timezone(TIMEZONE_NAME)
    now_local = datetime.now(timezone).strftime("%Y-%m-%d %I:%M %p CST")
    with col_ts:
        st.caption(f"🕐 Data loaded at {now_local} — auto-refreshes every 5 minutes")

    st.markdown("---")


# ==========================================================
# FILTERING
# ==========================================================
def initialize_filter_state() -> None:
    if "inv_clear_counter" not in st.session_state:
        st.session_state.inv_clear_counter = 0
    if "inv_group_mode" not in st.session_state:
        st.session_state.inv_group_mode = "Daily"


def render_filters(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, date, date, bool]:
    min_date = df_raw["date"].min().date()
    max_date = df_raw["date"].max().date()

    clear_key_suffix = st.session_state.inv_clear_counter

    col1, col2, col3, col4 = st.columns([1.5, 2, 3, 1])

    with col1:
        show_all_dates = st.toggle(
            "📋 Show All Dates",
            value=False,
            key=f"inv_show_all_{clear_key_suffix}",
        )

    with col2:
        selected_date = st.date_input(
            "Select Date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            disabled=show_all_dates,
            key=f"inv_date_{clear_key_suffix}",
        )

    with col3:
        supplier_options = sorted(df_raw["supplier"].dropna().unique())
        selected_suppliers = st.multiselect(
            "Supplier",
            options=supplier_options,
            default=[],
            placeholder="All suppliers",
            key=f"inv_supplier_{clear_key_suffix}",
        )

    with col4:
        st.markdown("<div style='padding-top:28px'>", unsafe_allow_html=True)
        if st.button("🔄 Clear", type="secondary", use_container_width=True):
            st.session_state.inv_clear_counter += 1
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    filtered_df = df_raw.copy()

    if show_all_dates:
        date_start = min_date
        date_end = max_date
    else:
        filtered_df = filtered_df[filtered_df["date"].dt.date == selected_date]
        date_start = selected_date
        date_end = selected_date

    if selected_suppliers:
        filtered_df = filtered_df[filtered_df["supplier"].isin(selected_suppliers)]

    return filtered_df, date_start, date_end, show_all_dates


# ==========================================================
# KPI / AGGREGATION
# ==========================================================
def calculate_kpis(df: pd.DataFrame) -> dict[str, float]:
    total_invoice = float(df["invoice_total"].sum())
    total_sale = float(df["total_sale"].sum())
    total_profit = total_sale - total_invoice
    profit_pct = (total_profit / total_sale * 100) if total_sale else 0.0

    return {
        "row_count": float(len(df)),
        "total_invoice": total_invoice,
        "total_sale": total_sale,
        "total_profit": total_profit,
        "profit_pct": profit_pct,
    }


def render_kpis(df: pd.DataFrame) -> None:
    metrics = calculate_kpis(df)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🧾 Invoice Rows", f"{int(metrics['row_count']):,}")
    col2.metric("💰 Total Invoice Cost", f"${metrics['total_invoice']:,.2f}")
    col3.metric("🏷️ Total Sale Value", f"${metrics['total_sale']:,.2f}")
    col4.metric(
        "📈 Total Profit",
        f"${metrics['total_profit']:,.2f}",
        delta=f"{metrics['profit_pct']:.1f}% margin",
    )

    st.markdown("---")


def add_period_column(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    df_copy = df.copy()

    if mode == "Daily":
        df_copy["Period"] = df_copy["date"].dt.strftime("%Y-%m-%d")
    elif mode == "Weekly":
        df_copy["Period"] = (
            df_copy["date"] - pd.to_timedelta(df_copy["date"].dt.weekday, unit="D")
        ).dt.strftime("Week of %b %d, %Y")
    else:
        df_copy["Period"] = df_copy["date"].dt.strftime("%B %Y")

    return df_copy


def build_grouped_summary(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    df_period = add_period_column(df, mode)

    grouped = (
        df_period.groupby(["supplier", "Period"], as_index=False)
        .agg(
            Entries=("invoice_total", "count"),
            Invoice_Total=("invoice_total", "sum"),
            Sale_Total=("total_sale", "sum"),
        )
    )

    grouped["Profit"] = grouped["Sale_Total"] - grouped["Invoice_Total"]
    grouped["Profit %"] = grouped.apply(
        lambda row: f"{row['Profit'] / row['Sale_Total'] * 100:.1f}%"
        if row["Sale_Total"]
        else "—",
        axis=1,
    )

    grouped = grouped.rename(
        columns={
            "supplier": "Supplier",
            "Entries": "# Entries",
            "Invoice_Total": "Invoice Total ($)",
            "Sale_Total": "Sale Total ($)",
            "Profit": "Profit ($)",
        }
    )

    grouped = grouped.sort_values(["Supplier", "Period"]).reset_index(drop=True)
    grouped.index += 1
    return grouped


def build_supplier_summary(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("supplier", as_index=False)
        .agg(
            Entries=("invoice_total", "count"),
            Invoice_Total=("invoice_total", "sum"),
            Sale_Total=("total_sale", "sum"),
        )
    )

    grouped["Profit"] = grouped["Sale_Total"] - grouped["Invoice_Total"]
    grouped["Profit %"] = grouped.apply(
        lambda row: f"{row['Profit'] / row['Sale_Total'] * 100:.1f}%"
        if row["Sale_Total"]
        else "—",
        axis=1,
    )

    grouped = grouped.rename(
        columns={
            "supplier": "Supplier",
            "Entries": "# Entries",
            "Invoice_Total": "Invoice Total ($)",
            "Sale_Total": "Sale Total ($)",
            "Profit": "Profit ($)",
        }
    )

    grouped = grouped.sort_values("Invoice Total ($)", ascending=False).reset_index(drop=True)
    grouped.index += 1
    return grouped


def render_dataframe_download_button(label: str, df: pd.DataFrame, filename: str) -> None:
    st.download_button(
        label=label,
        data=df.to_csv(index=False),
        file_name=filename,
        mime="text/csv",
        type="secondary",
        use_container_width=True,
    )


# ==========================================================
# GROUPED TOTALS SECTION
# ==========================================================
def render_group_mode_selector() -> str:
    st.markdown("### 📅 Grouped Totals")

    col_left, _ = st.columns([4, 6])
    with col_left:
        st.markdown(
            "<div style='padding-top:4px; font-size:14px; color:#555'>Group by</div>",
            unsafe_allow_html=True,
        )
        btn1, btn2, btn3 = st.columns(3)

        with btn1:
            if st.button(
                "📆 Daily",
                type="primary" if st.session_state.inv_group_mode == "Daily" else "secondary",
                use_container_width=True,
            ):
                st.session_state.inv_group_mode = "Daily"
                st.rerun()

        with btn2:
            if st.button(
                "📅 Weekly",
                type="primary" if st.session_state.inv_group_mode == "Weekly" else "secondary",
                use_container_width=True,
            ):
                st.session_state.inv_group_mode = "Weekly"
                st.rerun()

        with btn3:
            if st.button(
                "🗓️ Monthly",
                type="primary" if st.session_state.inv_group_mode == "Monthly" else "secondary",
                use_container_width=True,
            ):
                st.session_state.inv_group_mode = "Monthly"
                st.rerun()

    return st.session_state.inv_group_mode


def render_grouped_totals(df: pd.DataFrame, date_start: date, date_end: date) -> None:
    group_mode = render_group_mode_selector()
    grouped_df = build_grouped_summary(df, group_mode)

    st.info(
        f"Showing **{len(grouped_df):,}** rows across "
        f"**{grouped_df['Supplier'].nunique()}** supplier(s) — grouped **{group_mode}**"
    )

    st.dataframe(
        grouped_df,
        use_container_width=True,
        hide_index=False,
        height=460,
        column_config={
            "Invoice Total ($)": st.column_config.NumberColumn(format="$%.2f"),
            "Sale Total ($)": st.column_config.NumberColumn(format="$%.2f"),
            "Profit ($)": st.column_config.NumberColumn(format="$%.2f"),
        },
    )

    col_dl, _, _ = st.columns([2, 0.3, 7])
    with col_dl:
        render_dataframe_download_button(
            label="📥 Download Grouped Table (.csv)",
            df=grouped_df,
            filename=f"invoices_{group_mode.lower()}_{date_start}_{date_end}.csv",
        )

    st.markdown("---")


# ==========================================================
# SUPPLIER SUMMARY SECTION
# ==========================================================
def render_supplier_summary(df: pd.DataFrame, date_start: date, date_end: date) -> None:
    st.markdown("### 🏭 Supplier Summary")
    st.caption(f"Aggregated across {date_start} → {date_end}")

    supplier_summary = build_supplier_summary(df)

    st.dataframe(
        supplier_summary,
        use_container_width=True,
        hide_index=False,
        height=min(50 + len(supplier_summary) * 38, 460),
        column_config={
            "Invoice Total ($)": st.column_config.NumberColumn(format="$%.2f"),
            "Sale Total ($)": st.column_config.NumberColumn(format="$%.2f"),
            "Profit ($)": st.column_config.NumberColumn(format="$%.2f"),
        },
    )

    col_dl, _, _ = st.columns([2, 0.3, 7])
    with col_dl:
        render_dataframe_download_button(
            label="📥 Download Supplier Summary (.csv)",
            df=supplier_summary,
            filename=f"supplier_summary_{date_start}_{date_end}.csv",
        )


# ==========================================================
# MAIN
# ==========================================================
def main() -> None:
    initialize_filter_state()
    config = get_supabase_config()

    render_refresh_controls()

    df_raw = load_invoices_data(config)
    if df_raw.empty:
        st.error(
            "❌ No data found in the `invoices` table. "
            "Run **sync_invoices.py** to populate it from your Excel workbook."
        )
        st.stop()

    filtered_df, date_start, date_end, show_all_dates = render_filters(df_raw)

    if filtered_df.empty:
        st.warning("No invoice data matches the selected filters.")
        st.stop()

    render_kpis(filtered_df)

    if show_all_dates:
        render_grouped_totals(filtered_df, date_start, date_end)

    render_supplier_summary(filtered_df, date_start, date_end)


if __name__ == "__main__":
    main()
