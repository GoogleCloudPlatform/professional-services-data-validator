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
