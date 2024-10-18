# Release steps

Our release process is managed by Cloud Build. Please review the PSO Cloud Build project to view more granular build details.

# Manual Release steps

If a manual build is required, or you wish to run a build to a custom repository, please follow the instructions below.

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

## Prepare a release

- Review and merge PR from `release-please`. If anything needs adjusting,
  update the files in the PR. Check that it:
    - Updates version string in `setup.py`.
    - Includes all expected changes in `CHANGELOG.md`.

## Build & Publish a test package

- After the PR is merged, checkout the main `develop` branch.

  ```
  git fetch upstream --tags
  git checkout vA.B.C
  ```

- Remove any temporary files leftover from previous builds.

  ```
  git clean -xfd
  ```

- Build the package to the pypi test site

  ```
  PROJECT_ID=pso-kokoro-resources
  _TWINE_REPOSITORY_URL=https://test.pypi.org/legacy/
  _TWINE_USERNAME=<user>
  _TWINE_PASSWORD=<password>

  gcloud builds submit \
      --config ci/cloudbuild_pypi.yaml \
      --substitutions "_TWINE_REPOSITORY_URL=${_TWINE_REPOSITORY_URL},_TWINE_USERNAME=${_TWINE_USERNAME},_TWINE_PASSWORD=${_TWINE_PASSWORD}" \
      --project=${PROJECT_ID}
  ```

## Build & Publish Package

- After the PR is merged, checkout the main `develop` branch.

  ```
  git fetch upstream --tags
  git checkout vA.B.C
  ```

- Remove any temporary files leftover from previous builds.

  ```
  git clean -xfd
  ```

- Build the package to the pypi test site

  ```
  PROJECT_ID=pso-kokoro-resources
  _TWINE_REPOSITORY_URL=https://upload.pypi.org/legacy/
  _TWINE_USERNAME=<user>
  _TWINE_PASSWORD=<password>

  gcloud builds submit \
      --config ci/cloudbuild_pypi.yaml \
      --substitutions "_TWINE_REPOSITORY_URL=${_TWINE_REPOSITORY_URL},_TWINE_USERNAME=${_TWINE_USERNAME},_TWINE_PASSWORD=${_TWINE_PASSWORD}" \
      --project=${PROJECT_ID}
  ```

## Test the package

- Create a fresh venv.
- Install the package from the `dist/` directory.

  ```
  pip install --upgrade google_pso_data_validator
  ```

- Check that the command-line runs.

  ```
  data-validation -h
  python -m data_validation -h
  ```
