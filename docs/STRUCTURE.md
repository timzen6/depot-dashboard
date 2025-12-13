# Project Architecture & Structure

## 1. Core Principles
This project follows a strict separation of concerns to ensure maintainability of the Data Science logic and the UI.

* **ETL First:** Data is downloaded, cleaned, and stored in Parquet files *before* the app runs. The App never calls `yfinance` directly.
* **Atomic Storage:** To prevent data corruption, all file writes use the "Write-to-Temp -> Rename" pattern.
* **Type Safety:** Pydantic models in `src/core/models.py` are the single source of truth for data schemas.

## 2. Streamlit MVC Pattern
We use a modified Model-View-Controller pattern adapted for Streamlit.

### The Layers
1.  **Pages (`src/app/pages/`)**:
    * **Role:** The Controller/Router.
    * **Responsibility:** Initializes Session State, calls Logic to get data, passes data to View.
    * **Rule:** Max 50 lines of code. No data transformation here.

2.  **Logic (`src/app/logic/`)**:
    * **Role:** The Model / Business Logic.
    * **Responsibility:** Reads Parquet files, calculates KPIs (ROCE, TTM), handles caching.
    * **Output:** Returns clean Polars DataFrames or Pydantic objects to the Page.

3.  **Views (`src/app/views/`)**:
    * **Role:** The View.
    * **Responsibility:** Renders Charts (Plotly) and Tables (AgGrid/DataEditor).
    * **Input:** Receives data arguments from the Page. NEVER reads files directly.

### The "Complexity Scaler"
To keep the project clean, we handle file organization based on complexity:

* **Scenario A: Simple Feature (e.g., Overview)**
    * One file for Logic: `src/app/logic/overview.py`
    * One file for View: `src/app/views/overview.py`

* **Scenario B: Complex Feature (e.g., Stock Detail)**
    * A Folder (Package) for Logic:
        * `src/app/logic/stock_detail/__init__.py` (Main entry)
        * `src/app/logic/stock_detail/calculations.py`
    * A Folder (Package) for View:
        * `src/app/views/stock_detail/__init__.py` (Main render function)
        * `src/app/views/stock_detail/charts.py` (Plotly code)
        * `src/app/views/stock_detail/summary.py` (Text widgets)

## 3. Data Directory
* `data/input/`: Manual user data (Broker exports as CSV/XLSX, Watchlists).
* `data/prices/`: Automated OHLCV data (Parquet).
* `data/fundamentals/`: Automated Financials (Parquet).
