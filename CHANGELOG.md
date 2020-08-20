# Changelog

## 0.1.1 (Release date TBD)

### Bug Fixes

- update BigQuery dependencies to fix group-by results handler [#64](https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/64)

### Documentation

- remove references to unsupported validations from README [#63](https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/63)
- includes wheel file installation steps in README [#57](https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/57)
- add filters and data sources to README [#56](https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/56)

### Internal / Testing Changes

- move ibis addons to third-party directory [#61](https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/61)


## 0.1.0 (2020-07-16)

Initial alpha release.

### Features

- Add `data-validation` CLI, which can `run` from CLI arguments, `store` a
  configuration YAML file, or run from a `run-config` YAML file.
- Add support for querying Teradata.
- Add support for querying BigQuery.
- Write report output to BigQuery.

### Dependencies

- To use Teradata support, you must manually install the `teradatasql` PIP
  package.

### Documentation

- See the `README.md` file for getting started instructions.
