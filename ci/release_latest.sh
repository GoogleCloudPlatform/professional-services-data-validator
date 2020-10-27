#!/bin/bash
python3 -m venv rel_venv
source rel_venv/bin/activate

pip install setuptools wheel
python setup.py register sdist bdist_wheel

export PACKAGE_VERSION=$(grep 'version = ' setup.py | awk '{print $3;}' | sed 's/"//g')
export GCS_DIRECTORY=gs://professional-services-data-validator/releases/${PACKAGE_VERSION}/
pip install ./dist/google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl

data-validation -h
gsutil -m rm ${GCS_DIRECTORY}**

gsutil cp README.md CHANGELOG.md ${GCS_DIRECTORY}
gsutil cp dist/google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl ${GCS_DIRECTORY}google_pso_data_validator-${PACKAGE_VERSION}-py3-none-any.whl
gsutil cp dist/google-pso-data-validator-${PACKAGE_VERSION}.tar.gz ${GCS_DIRECTORY}google-pso-data-validator-${PACKAGE_VERSION}.tar.gz

gsutil -m acl ch -u AllUsers:R ${GCS_DIRECTORY}**
deactivate
rm -rf rel_venv/

# Push New Release to Latest

gsutil cp ${GCS_DIRECTORY}* gs://professional-services-data-validator/releases/latest/
