# execute it after the venv3-9 activation

pytest --quiet --cov=data_validation --cov=tests.unit --cov-append --cov-config=.coveragerc --cov-report=term tests/unit
flake8 data_validation tests
black --check data_validation samples tests noxfile.py setup.py

python3 -m nox --envdir ~/dvt/envs/ -s unit_small blacken lint

