from time import sleep

import polars as pl
import streamlit as st

from src.app.logic.data_loader import load_all_stock_data
from src.app.views.screener import key_to_selected_tickers
from src.core.admin_engine import AdminEngine

# Page config
st.set_page_config(
    page_title="Admin Console",
    page_icon="🛠️",
    layout="wide",
)

# ------------------------------------------------------------------
# Initialization & Data Loading
# ------------------------------------------------------------------


def custom_rerun() -> None:
    """Custom rerun to also invalidate cached resources."""
    load_all_stock_data.clear()
    st.rerun()


@st.cache_resource  # type: ignore[misc]
def get_admin_engine() -> AdminEngine:
    return AdminEngine()


engine = get_admin_engine()

dashboard_data, portfolio_dict, fx_engine = load_all_stock_data()

st.title("🛠️ Admin Console")

portfolios = engine.portfolio_manager.get_all_portfolios()

portfolio_key_name_dict = {p.display_name or k: k for k, p in portfolios.items()}
portfolio_display_names = sorted(list(portfolio_key_name_dict.keys()))

# ------------------------------------------------------------------
# Layout: Top Bar (Global Actions)
# ------------------------------------------------------------------

col_sel, col_actions = st.columns([1, 2], vertical_alignment="bottom")

with col_sel:
    portfolio_display_selection = st.selectbox(
        "Select Portfolio",
        options=portfolio_display_names,
    )
    portfolio_selection = portfolio_key_name_dict[portfolio_display_selection]

selected_portfolio = portfolios[portfolio_selection]
is_editable = selected_portfolio.is_editable


@st.dialog("Confirm Delete Portfolio")  # type: ignore[misc]
def show_delete_dialog(portfolio_name: str) -> None:
    st.warning(f"Are you sure you want to delete '{portfolio_name}'?")
    if st.button("Confirm Delete", type="primary"):
        engine.portfolio_manager.delete_portfolio(portfolio_name)
        st.success("Deleted!")
        custom_rerun()


@st.dialog("Create New Portfolio")  # type: ignore[misc]
def show_create_dialog() -> None:
    name = st.text_input("Name (ID):")
    display = st.text_input("Display Name:")
    if st.button("Create"):
        if name in engine.portfolio_manager.get_all_portfolios():
            st.error("Exists already.")
        else:
            engine.portfolio_manager.create_portfolio(name, display or None)
            st.success("Created!")
            custom_rerun()


with col_actions:
    st.write("### Actions")
    b1, b2, b3 = st.columns(3)
    with b1:
        if st.button("🔄 Update Data"):
            with st.spinner(f"Updating {portfolio_selection}..."):
                engine.update_portfolio_data(portfolio_selection)
            st.success("Updated!")
    with b2:
        if st.button("➕ New Portfolio"):
            show_create_dialog()
    with b3:
        if is_editable:
            if st.button("🗑️ Delete Portfolio", type="primary"):
                show_delete_dialog(portfolio_selection)
        else:
            st.caption("🔒 System Portfolio")

st.divider()

# ------------------------------------------------------------------
# Main Content: Split View (Editor & Add)
# ------------------------------------------------------------------

meta_col1, meta_col2 = st.columns([3, 2])

# --- LEFT: Portfolio Editor ---
with meta_col1:
    name_display = selected_portfolio.display_name or selected_portfolio.name
    st.subheader(f"Positions: {name_display}")

    meta = dashboard_data.metadata.select(["ticker", "short_name", "display_name"]).with_columns(
        pl.coalesce(pl.col("display_name"), pl.col("short_name"), pl.col("ticker")).alias("name")
    )
    latest_prices = (
        dashboard_data.prices.sort(["ticker", "date"])
        .group_by("ticker")
        .last()
        .select(["ticker", "close", "date", "currency"])
        .pipe(fx_engine.convert_to_target, "close", "currency")
    )

    positions = selected_portfolio.positions
    if not positions:
        st.info("Portfolio is empty.")
        df_pos = pl.DataFrame(
            {
                "ticker": [],
                "shares": [],
                "name": [],
                "close_EUR": [],
                "position_value_EUR": [],
            }
        )
    else:
        df_pos = (
            pl.DataFrame(positions)
            .join(meta, on="ticker", how="left")
            .join(latest_prices, on="ticker", how="left")
            .select(["ticker", "name", "shares", "close_EUR"])
            .with_columns((pl.col("shares") * pl.col("close_EUR")).alias("position_value_EUR"))
        )

    # --- UPDATED: Editable Table Logic ---
    if is_editable:
        st.caption("💡 Edit shares below. Set to **0** to remove. Click 'Save' to apply.")

        edited_df = st.data_editor(
            df_pos.to_pandas(),
            column_config={
                "ticker": st.column_config.TextColumn("Ticker", disabled=True),
                "name": st.column_config.TextColumn("Name", disabled=True),
                "shares": st.column_config.NumberColumn(
                    "Shares", min_value=0.0, step=1.0, required=True
                ),
                "close_EUR": st.column_config.NumberColumn(
                    "Price (EUR)", disabled=True, format="%.2f"
                ),
                "position_value_EUR": st.column_config.NumberColumn(
                    "Position Value (EUR)", disabled=True, format="%.2f"
                ),
            },
            use_container_width=True,
            num_rows="fixed",
            key=f"editor_{portfolio_selection}",
        )

        # --- NEW: Save Button Logic ---
        if st.button("💾 Save Changes", type="primary"):
            changes = 0
            current_map = {p.ticker: p.shares for p in positions}

            for _, row in edited_df.iterrows():
                ticker = row["ticker"]
                new_shares = row["shares"]
                old_shares = current_map.get(ticker)

                if old_shares is not None and new_shares != old_shares:
                    # Backend Logic handles deletion if shares <= 0
                    engine.portfolio_manager.update_position_share_count(
                        portfolio_selection, ticker, new_shares
                    )
                    changes += 1

            if changes > 0:
                st.success(f"Saved {changes} changes.")
                sleep(0.5)
                custom_rerun()
            else:
                st.info("No changes detected.")
    else:
        st.dataframe(df_pos, use_container_width=True)


# --- RIGHT: Add Ticker / Onboarding ---
with meta_col2:
    if is_editable:
        st.subheader("Add Position")

        # 1. Search Data Preparation
        latest_prices = dashboard_data.prices.sort(["ticker", "date"]).group_by("ticker").last()
        meta = (
            dashboard_data.metadata.join(
                latest_prices.select(["ticker", "close", "date"]),
                on="ticker",
                how="left",
            )
            .with_columns(
                pl.coalesce(pl.col("display_name"), pl.col("short_name"), pl.col("ticker")).alias(
                    "name"
                )
            )
            .pipe(fx_engine.convert_to_target, "close", "currency")
        )

        filter_text = st.text_input(
            "🔍 Search Ticker (Name, Symbol)", placeholder="e.g. MSFT"
        ).lower()

        if filter_text:
            filtered_meta = meta.filter(
                pl.col("ticker").str.to_lowercase().str.contains(filter_text)
                | pl.col("name").str.to_lowercase().str.contains(filter_text)
                | pl.col("country").str.to_lowercase().str.contains(filter_text)
            )
        else:
            filtered_meta = meta

        # 2. Selection Table
        st.dataframe(
            filtered_meta,
            column_order=["ticker", "name", "country", "close_EUR"],
            key="ticker_selection_table",
            on_select="rerun",
            selection_mode="single-row",
            height=300,
        )

        selected_rows = key_to_selected_tickers(
            "ticker_selection_table", filtered_meta, return_all_if_none=False
        )

        # --- NEW: Case A - Ticker Found ---
        if selected_rows:
            sel_ticker = selected_rows[0]
            st.markdown(f"**Add:** `{sel_ticker}`")

            with st.form("add_pos_form"):
                shares_input = st.number_input("Shares", min_value=0.01, value=1.0, step=1.0)
                if st.form_submit_button("Add to Portfolio"):
                    engine.portfolio_manager.add_ticker_to_portfolio(
                        portfolio_selection, sel_ticker, shares_input
                    )
                    st.success(f"Added {sel_ticker}")
                    custom_rerun()

        # --- NEW: Case B - Onboarding (New Ticker) ---
        elif filter_text and filtered_meta.is_empty():
            st.warning("No ticker found locally.")
            st.info("Do you want to fetch this ticker from Yahoo Finance?")

            new_ticker_input = st.text_input("Enter exact Yahoo Ticker:", value=filter_text.upper())

            if st.button("🚀 Fetch & Onboard"):
                with st.status("Onboarding...", expanded=True) as status:
                    try:
                        status.write("Fetching metadata...")
                        engine.init_new_ticker(new_ticker_input)
                        status.update(label="Done!", state="complete")
                        st.success(
                            f"Ticker {new_ticker_input} available! Please select it in the table."
                        )
                        st.cache_data.clear()
                        sleep(1)
                        custom_rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")
    else:
        st.info("Select an editable portfolio to manage positions.")
