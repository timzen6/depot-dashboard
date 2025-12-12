# Quality Core

A modern data engineering pipeline with an interactive Streamlit dashboard.

## Tech Stack

- **Python**: 3.11+
- **Package Manager**: uv
- **Data Processing**: Polars (method chaining, lazy evaluation)
- **Validation**: Pydantic V2 (strict typing)
- **Visualization**: Plotly, Streamlit
- **Data Source**: yfinance
- **Code Quality**: Ruff, mypy, pre-commit

## Project Structure

```
quality-core/
├── .github/
│   └── copilot-instructions.md
├── src/
│   ├── core/           # Domain models and configuration
│   ├── etl/            # Pipeline logic and transformations
│   └── app/            # Streamlit dashboard
│       └── views/      # UI components
├── data/
│   ├── raw/            # Raw data (gitignored)
│   ├── staging/        # Intermediate data (gitignored)
│   └── production/     # Final data (gitignored)
├── tests/              # Test suite
└── pyproject.toml
```

## Setup

### 1. Install uv (if not already installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Create virtual environment and install dependencies

```bash
# Create a virtual environment with Python 3.11+
uv venv --python 3.11

# Activate the virtual environment
source .venv/bin/activate  # macOS/Linux

# Install dependencies
uv pip install -e ".[dev]"
```

### 3. Setup pre-commit hooks

```bash
pre-commit install
```

## Usage

### Run the Dashboard

```bash
streamlit run src/app/main.py
```

### Code Quality

```bash
# Format and lint
ruff check . --fix
ruff format .

# Type checking
mypy src/

# Run tests
pytest
```

## Development Guidelines

- Use **Polars method chaining** for data transformations
- Apply **Pydantic V2** for data validation with strict typing
- Follow **mypy strict mode** (all functions must be typed)
- Keep line length to **100 characters**
- Use **pre-commit hooks** before committing

## License

MIT
