# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows [Google's Open Source Community
Guidelines](https://opensource.google/conduct/).

## Development environment

Clone repo locally and install via pip:

```
git clone git@github.com:GoogleCloudPlatform/professional-services-data-validator.git
cd professional-services-data-validator/
python -m venv env
source env/bin/activate
python -m pip install --upgrade pip
python -m pip install .
```

## Local Testing

This project uses [Nox](https://nox.thea.codes/en/stable/) for managing tests. Install nox to your local environment and it will handle creating the virtual environments required for each test.

To run our local testing suite, use:

`python3 -m nox --envdir ~/dvt/envs/ -s unit_small blacken lint`

See [our script](tests/local_check.sh) for using nox to run tests step by step.

You can also run pytest directly:
```python
pip install pyfakefs==4.6.2
pytest tests/unit
```

To lint your code, run:
```
pip install black==22.3.0
pip install flake8
black $BLACK_PATHS # Find this variable in our noxfile
flake8 data_validation
flake8 tests
```
The above is similar to our [noxfile lint test](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/noxfile.py).

## Conventional Commits

This project uses [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/) to manage the
CHANGELOG and releases.

Allowed commit prefixes are defined in the [release-please source
code](https://github.com/googleapis/release-please):

### User-facing commits

- `feat: ` section: 'Features'
- `fix: ` section: 'Bug Fixes'
- `perf: ` section: 'Performance Improvements'
- `deps: ` section: 'Dependencies'
- `revert: ` section: 'Reverts'
- `docs: ` section: 'Documentation'

### Hidden commits (not shown in CHANGELOG)

- `style: ` section: 'Styles', hidden: true
- `chore: ` section: 'Miscellaneous Chores', hidden: true
- `refactor: ` section: 'Code Refactoring', hidden: true
- `test: ` section: 'Tests', hidden: true
- `build: ` section: 'Build System', hidden: true
- `ci: ` section: 'Continuous Integration', hidden: true
