# Release steps

## Prepare a release

- Update [CHANGELOG.md](CHANGELOG.md) based on git commit history since last
  release.

  Template (include only those sections that apply):

  ```
  ### Features

  ### Bug Fixes

  ### Dependencies

  ### Documentation

  ### Internal / Testing Changes
  ```
- Update the version string in [setup.py](setup.py).
- Send a pull request with your changes.

## Build a package

- After the PR is merged, checkout the main `develop` branch.

  ```
  git fetch upstream develop
  git checkout develop
  git rebase -i upstream/develop
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

- Tag the release.

  ```
  git tag -a x.x.x -m 'Version x.x.x'
  ```

- Push the tag to GitHub.

  ```
  git push upstream develop --tags
  ```

- Find the tag in the [GitHub
  releases](https://github.com/GoogleCloudPlatform/professional-services-data-validator/releases).
  - Copy the release notes there.
  - Upload both files from the `dist/` directory.

- Upload the `.whl` wheel file, README, and CHANGELOG to [Google
  Drive](https://drive.google.com/corp/drive/folders/1C387pJKyqOCTN0I7sIm0SP6pfHu0PrLG).

- Release Version to GCS


```
export PACKAGE_VERSION=X.X.X
gsutil cp README.md CHANGELOG.md dist/google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl dist/google-pso-data-validator-${PACKAGE_VERSION}.tar.gz gs://professional-services-data-validator/releases/${PACKAGE_VERSION}/
```

- Release Latest to GCS

```
export PACKAGE_VERSION=X.X.X

gsutil cp README.md CHANGELOG.md gs://professional-services-data-validator/releases/latest/
gsutil cp dist/google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl gs://professional-services-data-validator/releases/latest/google_pso_data_validator-latest-py3-none-any.whl
gsutil cp dist/google-pso-data-validator-${PACKAGE_VERSION}.tar.gz gs://professional-services-data-validator/releases/latest/google-pso-data-validator-latest.tar.gz
```

gsutil -m acl ch -u AllUsers:R gs://professional-services-data-validator/**
