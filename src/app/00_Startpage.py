"""Quality Core Dashboard - Main Entry Point.

Welcome page for the Streamlit application.
Navigate to specific pages using the sidebar.
"""

import streamlit as st

st.set_page_config(
    page_title="Quality Core Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Header
st.title("📊 Quality Core Dashboard")
st.divider()

# Welcome message
st.markdown(
    """
    ## Welcome to Your High-Conviction Portfolio Dashboard

    This application provides comprehensive analysis of your
    investment portfolios and individual stocks.

    ### Available Pages:

    **📊 Overview**

    Track portfolio performance, positions, and composition
    across multiple strategies.

    **🔍 Stock Detail**

    Deep dive into individual stock metrics including:
    - Price history and volume analysis
    - Valuation metrics (FCF Yield, Dividend Yield)
    - Quality indicators (ROCE, Free Cash Flow)

    ### Getting Started:

    1. Use the sidebar to navigate between pages
    2. Select a portfolio or stock from the dropdown
    3. Adjust date ranges and filters as needed

    ---

    **Data Source:** Production data from `data/prod/`
    **Last Update:** Data is cached for 1 hour and refreshed automatically
    """
)

# Data status sidebar
st.sidebar.title("Navigation")
st.sidebar.info(
    "Select a page from the sidebar to begin analysis.\n\n"
    "All data is loaded with calculated metrics on demand."
)
