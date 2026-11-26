# General Copilot Instructions

Reusable conventions for Python data projects. Copy this file into new projects and extend it with project-specific rules in `copilot-instructions.md`.

## Critical Guidelines

### 1. Polars & Data
- **Mode:** Use **Eager execution** (`DataFrame`) by default for readability. Use `LazyFrame` only for `scan_parquet` or heavy aggregations.
- **Style:** Strictly use **method chaining**.
- **Pandas:** Only allowed at the I/O boundary. Convert to Polars immediately.

### 2. Configuration
- Use **Pydantic Settings** (`BaseSettings`) for solid, validated configuration (env vars / YAML files), not ad-hoc dicts.

### 3. Logging
- Use a proper logger (e.g., **loguru**) **initialized once** at the application entry point. Import and use it everywhere — never reconfigure it inside modules.

### 4. File I/O
- **Atomic Writes:** Always write to `.tmp` and rename to target path.
- **Paths:** Use `pathlib.Path`.

### 5. Documentation
- Keep the project overview, architecture, and setup instructions in **README.md**. Reference it from instruction files instead of duplicating the content.

## Best Practices

- **Don't overengineer:** These are private projects. Avoid excessive try/catch blocks and defensive checks — let errors surface. Strive for readability over precautions.
- **Typing:** Always use type hints.
- **Docstrings:** Document functions with short docstrings — only explain the *why* and complicated parts. Do not redundantly document input and return types if type hints are clear.
- **Style:** Keep functions pure where possible. Prefer a functional style.

## AI Collaboration & Tooling

- **Directness:** Skip pleasantries and apologies. Jump straight into the solution. Do not explain basic Python concepts.
- **Context Management Caveat:** When using tools like Aider that specifically need adding files to the context manually:
  - Distinguish strictly between context and edits.
  - If you only need to inspect a file to understand an interface, schema, or configuration, ask me to add it as read-only (e.g., via `/read <file>`).
  - Only request editable access (e.g., via `/add <file>`) for files you actively intend to modify.
