"""Verification script for WI-08 Streamlit application structure.

Checks that all required files exist and follow the MVC architecture.
"""

from pathlib import Path


def verify_structure() -> bool:
    """Verify the Streamlit app structure is complete.

    Returns:
        True if all required files exist, False otherwise
    """
    base = Path("src/app")

    required_files = [
        # Entry point
        base / "main.py",
        # Logic layer
        base / "logic" / "__init__.py",
        base / "logic" / "data_loader.py",
        base / "logic" / "overview.py",
        base / "logic" / "stock_detail" / "__init__.py",
        base / "logic" / "stock_detail" / "loader.py",
        # View layer
        base / "views" / "__init__.py",
        base / "views" / "common.py",
        base / "views" / "overview.py",
        base / "views" / "stock_detail" / "__init__.py",
        base / "views" / "stock_detail" / "charts.py",
        # Page layer
        base / "pages" / "01_overview.py",
        base / "pages" / "02_stock_detail.py",
    ]

    missing = []
    for file_path in required_files:
        if not file_path.exists():
            missing.append(str(file_path))

    if missing:
        print("❌ Missing files:")
        for m in missing:
            print(f"  - {m}")
        return False

    print("✅ All required files exist!")
    print("\n📁 Structure Summary:")
    print(f"  Logic Layer:  {len([f for f in required_files if 'logic' in str(f)])} files")
    print(f"  View Layer:   {len([f for f in required_files if 'views' in str(f)])} files")
    print(f"  Page Layer:   {len([f for f in required_files if 'pages' in str(f)])} files")
    print(f"  Total:        {len(required_files)} files")

    return True


if __name__ == "__main__":
    import sys

    success = verify_structure()
    sys.exit(0 if success else 1)
