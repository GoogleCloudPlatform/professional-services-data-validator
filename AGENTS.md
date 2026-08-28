# Project Guidelines - Data Validation Tool (DVT)

## 1. Environment & Setup
* Always activate the local virtual environment before running commands, formatting, linting, or tests:
  ```bash
  source env/bin/activate
  ```

---

## 2. Code Formatting & Linting
All Python code must be formatted with Black and pass Flake8 linting before committing:

* **Black (Code Formatting)**:
  ```bash
  black data_validation tests third_party samples setup.py noxfile.py
  ```
* **Flake8 (Linting)**:
  ```bash
  flake8 data_validation tests
  ```
* Or via Nox:
  ```bash
  python3 -m nox -s blacken lint
  ```

---

## 3. Running Tests

### Unit Tests
Unit tests run locally with `pyfakefs` and mocked connections:
```bash
# Run all unit tests
pytest tests/unit

# Run a specific unit test file
pytest tests/unit/test_combiner.py
```

### System / Integration Tests
Integration tests run against live cloud data sources:

* **BigQuery Integration Tests**:
  ```bash
  PROJECT_ID=pso-kokoro-resources pytest tests/system/data_sources/test_bigquery.py
  ```
* **Other Data Sources**:
  ```bash
  pytest tests/system/data_sources/test_<source>.py
  ```

---

## 4. Git & Commit Guidelines
* Follow [Conventional Commits](https://www.conventionalcommits.org/):
  * `feat: ` New features
  * `fix: ` Bug fixes
  * `chore: ` Chores, cleanup, dependency updates
  * `refactor: ` Code refactoring without behavior change
  * `test: ` Adding or updating tests
  * `docs: ` Documentation changes
