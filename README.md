# Quality Core 🚀

**Quality Core** is an advanced Portfolio Management and Analytics Dashboard designed for quality and esg focused investing.
There is also functionality on customizable **factor investing** for monitoring nuanced data-inspired investment strategies.

It combines a robust, local-first ETL pipeline for financial data ingestion with a clean, interactive Streamlit frontend for investment decision-making.

---

## 🏗 Tech Stack

* **Language:** Python 3.11+
* **Dependency Management:** [`uv`](https://github.com/astral-sh/uv) (Modern, fast pip replacement)
* **Frontend:** Streamlit (MVC Pattern)
* **Data Processing:** Polars, Pandas (Method Chaining), NumPy
* **Data Source:** `yfinance` (ETL pipeline), OpenBB (optional)
* **Storage:** Local Parquet Data Lake (Atomic writes)
* **Quality Assurance:** Ruff, MyPy, Pre-commit

---

## ✨ Key Features

### 📊 Dashboard (Streamlit)
* **Portfolio Overview:** Track performance, dividends, and asset allocation.
* **Asset Details:** Deep dive into specific stocks with fundamental analysis.
* **Screener:** Filter stocks based on custom factors (e.g., ROCE, FCF Yield, Margins).
* **Factor Analysis:** Visualizing "Quality," "Stability," "Real Assets," and "Price" scores.

### ⚙️ ETL Engine & CLI
* **Incremental Updates:** Smart fetching of missing price data (Gap Detection).
* **Snapshotting:** Historical archiving of fundamental data.
* **Data Lake:** Structured storage in `data/prod/`.

---

## 🚀 Getting Started

### Prerequisites
* **Python 3.11+**
* **uv** (Recommended installer)
* **Make** (Optional, for simplified commands)

**Install uv:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Installation

1. **Clone the repository:**
```bash
git clone https://github.com/timzen6/quality-core.git
cd quality-core
```

2. **Setup Environment:** The project uses `uv` to manage the virtual environment automatically.

Optional: Run sync manually to initialize the environment for your IDE (e.g., VS Code IntelliSense):
```bash
uv sync
```

3. **Configuration:** Copy the example environment file and adjust settings if necessary.
```bash
cp .env.example .env
```

---

## 🖥 Usage

The project includes a Makefile to streamline common tasks.

### 1. Run the Dashboard

Starts the Streamlit application. Dependencies are handled automatically.
```bash
make app
```

(Alternative: `uv run streamlit run src/app/00_Startpage.py`)

### 2. Run the ETL Pipeline (qc)

The project includes a CLI tool named `qc` (Quality Core) to manage data operations.


**Update Prices & Fundamentals:**
```bash
uv run qc etl
```

**Show all available commands:**
```bash
uv run qc --help
```

or preferably use **make** to set up your local database:
Use
```bash
make etl
```
to load or update all relevant data in user defined portfolios.
To make a full update to also load all preconfigured tickers run:
```bash
make etl-full
```
This update may take some time. Data updates can also be triggered within
the UI on the Admin page.



---

## 📂 Project Structure

```
.
├── config/                 # YAML configs (ETFs, Factors, General settings)
├── data/                   # Data storage (prod/staging) - .gitignored
├── docs/                   # Documentation & Architecture Decisions
├── src/
│   ├── analysis/           # Financial math & metric calculations
│   ├── app/                # Streamlit Application (MVC Pattern)
│   │   ├── 00_Startpage.py # << APP ENTRY POINT
│   │   ├── logic/          # Controllers/Business Logic
│   │   ├── pages/          # Streamlit Pages
│   │   └── views/          # UI Components
│   ├── core/               # Domain Models, Strategy Engine
│   ├── etl/                # Extract-Transform-Load pipelines
│   └── main.py             # << CLI ENTRY POINT
├── Makefile                # Command shortcuts
├── pyproject.toml          # Project configuration & Dependencies
└── uv.lock                 # Dependency Lockfile
```

---

## 🛠 Development

We adhere to strict coding standards using `ruff` (linting/formatting) and `mypy` (static type checking).

**Run Quality Checks (Lint, Format, Type Check):**
```bash
make qc
```

---


## 📝 License

Distributed under the MIT License. See `LICENSE` for more information.
