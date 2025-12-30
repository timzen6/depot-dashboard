"""Data extraction layer with robust error handling and retry logic.

Wraps yfinance API calls with tenacity for network resilience.
Validates responses before returning to caller.
"""

from datetime import date

import pandas as pd
import yfinance as yf
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from src.core.domain_models import ReportType


class DataExtractor:
    """Handles all external data fetching from yfinance with retry logic."""

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore[misc]
    def get_ticker_info(self, ticker: str) -> dict[str, str]:
        """
        Fetches metadata like currency.
        Includes retry logic because yfinance .info endpoint is flaky.
        """
        try:
            # fast_info is often faster and more stable than .info
            # but .info has more details. For currency, fast_info is often sufficient.
            yf_ticker = yf.Ticker(ticker)

            # Try fast_info first, then info (fallback)
            currency = yf_ticker.fast_info.get("currency")
            if not currency:
                currency = yf_ticker.info.get("currency", "USD")

            return {"currency": currency}

        except Exception as e:
            logger.warning(f"[{ticker}] Failed to fetch info: {e}. Defaulting to USD.")
            return {"currency": "USD"}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )  # type: ignore[misc]
    def get_full_ticker_info(self, ticker: str) -> dict[str, str]:
        """
        Fetches full ticker info for discovery/debugging purposes.
        No retry logic here; caller can implement if needed.
        """
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info
        info["ticker"] = ticker
        return dict(info)

    def get_full_ticker_calendar(self, ticker: str) -> dict[str, str]:
        """
        Fetches full ticker calendar for discovery/debugging purposes.
        No retry logic here; caller can implement if needed.
        """
        yf_ticker = yf.Ticker(ticker)
        cal = yf_ticker.calendar
        if cal is None:
            return {"ticker": ticker}
        cal["ticker"] = ticker
        return dict(cal)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )  # type: ignore[misc]
    def get_prices(self, ticker: str, start_date: date) -> pd.DataFrame:
        """
        Fetch historical price data for a ticker starting from a specific date.

        Uses yfinance.download with auto_adjust=True to handle splits/dividends.
        Retries up to 3 times with exponential backoff for network stability.

        Args:
            ticker: Stock ticker symbol (e.g., "MSFT", "MUV2.DE")
            start_date: Start date for historical data (inclusive)

        Returns:
            pandas DataFrame with OHLCV data from yfinance

        Raises:
            ValueError: If no data is returned (invalid ticker or date range)
            Exception: Network or yfinance errors after 3 retry attempts
        """
        logger.info(f"[{ticker}] Fetching prices from {start_date}")

        try:
            price_data = yf.download(
                ticker,
                start=start_date,
                progress=False,
                auto_adjust=True,
                actions=True,
            )

            if price_data.empty:
                msg = f"No price data found for {ticker} starting from {start_date}"
                logger.warning(msg)
                raise ValueError(msg)

            logger.success(f"[{ticker}] Fetched {len(price_data)} price rows")
            return price_data

        except Exception as e:
            logger.error(f"[{ticker}] Failed to fetch prices: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )  # type: ignore[misc]
    def get_financials(
        self,
        ticker: str,
        report_type: ReportType = ReportType.ANNUAL,
    ) -> pd.DataFrame:
        """
        Fetch fundamental financial data for a ticker.

        Retrieves and merges balance sheet, income statement, and cash flow
        into a single DataFrame (metrics as rows, dates as columns).
        Retries up to 3 times with exponential backoff for network stability.

        Args:
            ticker: Stock ticker symbol (e.g., "MSFT", "MUV2.DE")
            report_type: Type of financial report (ANNUAL or QUARTERLY)

        Returns:
            pandas DataFrame with merged financial statements

        Raises:
            ValueError: If all statements are empty (invalid ticker or no data)
            Exception: Network or yfinance errors after 3 retry attempts
        """
        logger.info(f"[{ticker}] Fetching fundamental data")

        try:
            yf_ticker = yf.Ticker(ticker)

            if report_type == ReportType.ANNUAL:
                # Fetch all three financial statements
                inc = yf_ticker.financials
                bal = yf_ticker.balance_sheet
                cash = yf_ticker.cashflow
            elif report_type == ReportType.QUARTERLY:
                inc = yf_ticker.quarterly_financials
                bal = yf_ticker.quarterly_balance_sheet
                cash = yf_ticker.quarterly_cashflow
            else:
                raise ValueError(f"Unsupported report type: {report_type}")

            # Validate that we got at least some data
            if inc.empty and bal.empty and cash.empty:
                msg = f"No fundamental data found for {ticker} with report type {report_type}"
                logger.warning(msg)
                raise ValueError(msg)

            # Merge vertically (rows are metrics, columns are dates)
            # Remove duplicate metrics (e.g., Net Income appears in multiple statements)
            combined = pd.concat([inc, bal, cash])
            combined = combined[~combined.index.duplicated(keep="first")]

            logger.success(f"[{ticker}] Fetched {len(combined)} fundamental metrics")
            return combined

        except Exception as e:
            logger.error(f"[{ticker}] Failed to fetch fundamentals: {e}")
            raise
