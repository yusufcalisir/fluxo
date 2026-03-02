# Contributing to Fluxo

First off, thank you for considering contributing to Fluxo! It's people like you that make Fluxo such a great tool.

## Where do I go from here?

If you've noticed a bug or have a question, [search the issue tracker](https://github.com/fluxo-dev/fluxo/issues) to see if someone else in the community has already created a ticket. If not, go ahead and [make one](https://github.com/fluxo-dev/fluxo/issues/new/choose)!

## Setting up your development environment

1. Fork the repository on GitHub.
2. Clone your fork locally:
   ```bash
   git clone https://github.com/your-username/fluxo.git
   cd fluxo
   ```
3. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e ".[dev]"
   ```

## Running Tests

We use `pytest` for testing. To run the test suite:

```bash
pytest tests/ -v
```

Please ensure all tests pass before submitting a Pull Request. If you are adding new features, please include tests that cover them.

## Submitting a Pull Request

1. Create a new branch: `git checkout -b my-feature-branch`
2. Make your changes and commit them: `git commit -m "Add some feature"`
3. Push to the branch: `git push origin my-feature-branch`
4. Submit a pull request to the `main` branch.

Please provide a clear and concise description of your changes in the PR description.
