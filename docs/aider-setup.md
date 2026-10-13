# Aider Integration Guide

## What is Aider?
Aider is an AI-powered pair programming tool that integrates with our codebase to provide intelligent code assistance while maintaining our project's specific patterns and architecture. It connects to our configured LLM (Mistral) to understand the codebase context and make targeted changes.

## Why We're Testing Aider
We're evaluating Aider as a strategic alternative to GitHub Copilot for several key reasons:

1. **Token Efficiency**: Aider is designed to make more efficient use of LLM tokens by:
   - Focusing on specific files and changes rather than continuous inline suggestions
   - Using git-aware context to minimize unnecessary token usage
   - Providing more targeted, complete solutions rather than incremental suggestions

2. **Mistral Evaluation**: This integration serves as a practical test of:
   - Mistral's code understanding capabilities
   - The quality of its Python and domain-specific suggestions
   - How well it adapts to our project's architecture patterns
   - Cost-performance compared to other LLM providers

3. **Workflow Integration**: We want to assess:
   - How well Aider fits into our existing development workflow
   - Its ability to maintain our project's coding standards
   - The quality of its git integration for change management

## How Aider Works in Our Setup
1. **Context Understanding**: Aider uses our project's git repository and configuration files to understand the codebase structure and coding standards.
2. **Targeted Changes**: When requesting changes, Aider identifies exactly which files need modification based on our project's architecture patterns.
3. **Code Generation**: It generates complete file contents following our specific coding conventions (Polars method chaining, MVC separation, etc.).
4. **Review Process**: Changes are presented for review before being committed to maintain code quality.

## Our Specific Setup
Our project uses a specialized Aider configuration that integrates with:
- **uv** for Python package management
- **Mistral** as the LLM provider
- **Makefile** for common development tasks
- **Project-specific architecture patterns**

### Installation
1. First ensure you have the project dependencies installed:
   ```bash
   uv sync
   ```

2. The project includes a pre-configured Aider setup:
   - `.aider.conf.yaml` contains our project-specific settings
   - `.env` file provides the Mistral API key
   - Makefile provides the `aider` command

### Running Aider
Use the project's Makefile command to start Aider with our pre-configured settings:
```bash
make aider
```

This command:
- Uses uv to run Aider in our project environment
- Loads the Mistral API key from `.env`
- Applies our project-specific Aider configuration
- Maintains our coding standards and patterns

## Configuration Details
Our `.aider.conf.yaml` specifies:
- Mistral as the LLM provider (for our evaluation)
- Project-specific file patterns to include/exclude
- Coding style preferences that match our project
- Context settings optimized for our codebase size
- Token-efficient settings to maximize our evaluation budget

## Testing Approach with Aider
When using Aider in our project, we're specifically evaluating:

1. **Token Efficiency Metrics**:
   - Tokens used per meaningful change
   - Context window utilization
   - Cost per feature implemented

2. **Code Quality**:
   - Changes maintain our Polars method chaining pattern
   - New code follows our MVC architecture (logic in `src/app/logic`, views in `src/app/views`)
   - Type hints are properly used throughout
   - Changes respect our domain models and Pydantic validation

3. **Mistral Performance**:
   - Quality of code suggestions in our domain
   - Understanding of our project architecture
   - Ability to work with our specific tech stack (Polars, Streamlit, etc.)
   - Handling of complex financial calculations

4. **Integration**:
   - How well new features work with our existing data pipeline
   - Streamlit UI component integration with our view layer
   - Changes respect our file organization patterns

5. **Performance**:
   - Data transformations remain efficient (prefer Polars over Pandas)
   - Caching decorators are used appropriately for expensive operations
   - No unnecessary data copies are created

6. **Documentation**:
   - New functions include concise docstrings explaining the "why"
   - Complex logic is clearly commented
   - Changes to domain models are properly documented
