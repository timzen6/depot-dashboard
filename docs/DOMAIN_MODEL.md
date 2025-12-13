# Domain Model Definitions

## Core Entities (`src/core/models.py`)

### 1. AssetMeta
Static data about a holding.
- `ticker`: (str) Unique Identifier (e.g., "MSFT").
- `asset_type`: (Enum) STOCK or ETF.
- `currency`: (str) ISO Code (default EUR).

### 2. PriceRecord
Daily OHLCV data.
- `date`: (date)
- `close`: (float)
- `adjusted_close`: (float) -> Primary field for performance calc.
- `volume`: (int)

### 3. FundamentalRecord (Long Format)
Financial data points.
- `date`: (date) Reporting date.
- `metric`: (Enum) `FinancialMetric` (e.g., 'TotalRevenue', 'FreeCashFlow').
- `value`: (float) The actual number.
- `period`: (str) Usually '12M' or 'TTM'.

## Enums (`src/core/types.py`)

### FinancialMetric
Maps raw API keys to internal standard names.
- `TotalRevenue`
- `NetIncome`
- `OperatingCashFlow`
- `CapitalExpenditure`
- ... (Extend as needed)
