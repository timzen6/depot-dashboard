# GitHub Copilot Instructions for Quality Core

## Project Guidelines

### Code Style & Standards
- **Python Version**: 3.11+
- **Type Hints**: Use strict typing throughout the codebase (enforced by mypy)
- **Linting**: Ruff configured with line-length=100

### Core Libraries & Patterns

#### Polars
- Use **method chaining** for data transformations
- Prefer lazy evaluation with `.lazy()` and `.collect()` where appropriate
- Use Polars expressions (`.pl.col()`, `.pl.lit()`, etc.)
- Example:
  ```python
  result = (
      df.lazy()
      .filter(pl.col("value") > 0)
      .group_by("category")
      .agg(pl.col("amount").sum())
      .collect()
  )
  ```

#### Pydantic
- Use **Pydantic V2** syntax and features
- Leverage `Field()` for validation and metadata
- Use `ConfigDict` for model configuration
- Example:
  ```python
  from pydantic import BaseModel, Field, ConfigDict

  class DataModel(BaseModel):
      model_config = ConfigDict(strict=True, frozen=True)

      name: str = Field(..., min_length=1)
      value: float = Field(..., gt=0)
  ```

#### Streamlit
- Keep UI components in `src/app/views/`
- Use session state for data persistence
- Apply caching with `@st.cache_data` and `@st.cache_resource`

### Project Structure
- `src/core/`: Domain models and configuration (Pydantic models)
- `src/etl/`: Pipeline logic (Polars transformations)
- `src/app/`: Streamlit dashboard and views
- `tests/`: Unit and integration tests

### Best Practices
- Always use type hints
- Document functions with short docstrings, only say the why and explain complicated parts, we do not need to all input and return types documented
- Keep functions pure where possible, prefer functional style
- Follow method chaining pattern for data transformations
- strive for readability, rather than excessive checking and precautions
