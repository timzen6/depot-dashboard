import yfinance as yf

ticker = yf.Ticker("MSFT")
print("--- Financials Index (Rows) ---")
print(ticker.financials.index)
