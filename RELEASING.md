# Release steps

## Prerequisites

First, prepare a development environment. Follow the instructions in the
[contributing guide](CONTRIBUTING.md) to setup your virtual environment,
install the package locally, and add any dependencies needed for testing such
as nox or pytest.

1. Create PyPI (https://pypi.org/) and PyPI Test (https://test.pypi.org/) accounts. Example: https://pypi.org/user/TimSwast/

2. Configure PyPI with a `.pypirc` file: https://packaging.python.org/specifications/pypirc/#using-a-pypi-token

3. Have a remote named "upstream" pointing at this copy on GitHub.

    ```
    git remote add upstream git@github.com:GoogleCloudPlatform/professional-services-data-validator.git
    ```

4. Install `setuptools`, [twine](https://twine.readthedocs.io/en/latest/) CLI
   for PyPI, and `wheel` into your development virtual environment.

    ```
    pip install setuptools twine wheel
    ```

## Prepare a release

- Review and merge PR from `release-please`. If anything needs adjusting,
  update the files in the PR. Check that it:
    - Updates version string in `setup.py`.
    - Includes all expected changes in `CHANGELOG.md`.

## Build a package

- After the PR is merged, checkout the main `develop` branch.

  ```
  git fetch upstream --tags
  git checkout vA.B.C
  ```

- Remove any temporary files leftover from previous builds.

  ```
  git clean -xfd
  ```

- Build the package.

  ```
  python setup.py register sdist bdist_wheel
  ```

## Test the package

- Create a fresh venv.
- Install the package from the `dist/` directory.

  ```
  pip install ./dist/google_pso_data_validator-X.X.X-py3-none-any.whl
  ```

- Check that the command-line runs.

  ```
  data-validation -h
  python -m data_validation -h
  ```

## Publish the package

- [Optional] Upload to test PyPI. See [documentation for
  TestPyPI](https://packaging.python.org/guides/using-testpypi/)

    ```
    twine upload --repository testpypi dist/*
    ```

- [Optional] Try out test PyPI package

    ```
    pip install --upgrade \
      --index-url https://test.pypi.org/simple/ \
      --extra-index-url https://pypi.org/simple \
      google-pso-data-validator
    ```

- Upload to PyPI

    ```
    twine upload dist/*
    ```

- Find the tag in the [GitHub
  releases](https://github.com/GoogleCloudPlatform/professional-services-data-validator/releases).
  - Upload both files from the `dist/` directory.
