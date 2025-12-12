"""ETL Pipeline - Core data extraction and transformation."""

import polars as pl


def main() -> None:
    """Main ETL pipeline entry point."""
    print("Hello from ETL Pipeline!")
    print(f"Polars version: {pl.__version__}")
    print("yfinance ready for data extraction")


if __name__ == "__main__":
    main()
