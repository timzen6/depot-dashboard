import yfinance as yf

# Debug Apple Data Structure
ticker = yf.Ticker("AAPL")
fin = ticker.financials
bs = ticker.balance_sheet
cf = ticker.cashflow

print("--- INCOME STATEMENT KEYS ---")
print(fin.index.tolist())

print("\n--- BALANCE SHEET KEYS ---")
print(bs.index.tolist())

print("\n--- CASH FLOW KEYS ---")
print(cf.index.tolist())
