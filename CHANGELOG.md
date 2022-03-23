# Changelog

## Untagged

## [1.7.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.6.0...v1.7.0) (2022-03-23)


### Features

* deploy flask app via CLI ([#344](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/344)) ([b1dc82a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b1dc82adf92adf19702f5ef41590c62c7c128c74))
* first class support for row level hashing ([#345](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/345)) ([3d78ee5](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/3d78ee578b4222a9bdb19c7091445f48f413b9a0))
* GCS support for validation configs ([#340](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/340)) ([b09cd29](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b09cd29e6cad63462aacfbcc0c2d8fd819076f52))
* Hive hash function support ([#392](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/392)) ([0ca0ccf](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0ca0ccf88d9d6014ed363b9a6dc40d78afdf88ee))
* Hive partitioned tables support ([#375](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/375)) ([8f1af27](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/8f1af27ee57d53c736191f67219ca175e149d48f))
* Issue339 ldap logmech ([#347](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/347)) ([ad7f1fc](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ad7f1fcfc10541cf19044a1bf88d16deb1398772))
* Random Row Validation Logic ([#357](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/357)) ([229d870](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/229d870a4ac66fc2ff9a4ab256190a62674441ad))


### Bug Fixes

* add to_hex for bigquery hash ([#400](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/400)) ([e5c7ded](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e5c7ded2227eef97f024b93af2b949cc2cddbe93))
* Comparison fields Key Error fix ([#396](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/396)) ([a597b56](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a597b560ec2420127186157ae6fe0aef07f3b444))
* ensure all statuses are success or fail, particularly after _join_pivots ([#329](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/329)) ([#370](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/370)) ([310747d](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/310747d4a1aa1ed82e3f403959af3592478007d8))
* make status values consistent across validation types ([#377](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/377)) ([#378](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/378)) ([5c08463](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5c084633708b13f7eb4b505749dd52d0f43617cc))
* Multiple updates ([#359](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/359)) ([6b2614d](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/6b2614dcfae3ca67d85221a9ceab3881c5c6b30d))
* revert change from [#345](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/345) that causes filters, threshold and labels to be ignored for column validations ([#376](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/376)) ([#379](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/379)) ([8b295cf](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/8b295cf9746559fa48a448bd44fc6e4094820796))
* Status when source and target agg values are 0  ([#393](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/393)) ([6a41f68](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/6a41f681a4be63f0ab3afc34000d78c6f0df6087))
* support schema validation for more clients ([#355](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/355)) ([#380](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/380)) ([ed46295](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ed4629594eeaf7ca3a3a914b4c07e1b9c977f07c))
* supporting non default schemas for mssql ([#365](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/365)) ([100b3ea](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/100b3eabed5ca83245e10e40950e725155332dcd))
* test for nan when calculating fail/success in combiner ([#341](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/341)) ([#366](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/366)) ([a9720c2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a9720c2cff2c3713ad1de0d6573b4e86c06bcc65))
* use an appropriate column filter list for schema validation ([#350](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/350)) ([#371](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/371)) ([806151a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/806151a4162ae7320c602911692ee1ef861be027))


### Documentation

* Add Hive as a supported data source to docs ([#354](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/354)) ([be2a49d](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/be2a49d8849982f8f75f4bddc607e744b52ec180))

## [1.6.0](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.5.0...v1.6.0) (2021-12-01)


### Features

* teradata hashing implementation ([#324](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/324)) ([b74e03e](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b74e03ee94b666afe6a7765add81e6bfef9c227b))


### Bug Fixes

* Include StringIO into teradata ibis compiler.py ([#336](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/336)) ([1dba63b](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1dba63be87a8e4b4ff63d1753c6197a6ec3411e5))
* Issue348 casting ([#349](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/349)) ([1560c7e](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1560c7e4a939f6b1d6eaae912d46ace6eda422b6))


### Documentation

* add local development nox docs ([#342](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/342)) ([80d26c6](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/80d26c6adb02ce0d4eeb9053b50f1cdad8372f2c))

## [1.5.0](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.4.0...v1.5.0) (2021-10-19)


### Features

* added kerberos service name flag for Impala connections, fixed bug in row validation with YAML ([#320](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/320)) ([351994c](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/351994c4fd028540915694e8f154ef45cdcd6398))
* Track DVT GCS connections ([#326](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/326)) ([b384b1f](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b384b1fc62d24b8761d5587cd5fee5ff0c808459))


### Bug Fixes

* Issue323 row hash ([#328](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/328)) ([1a03ad7](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1a03ad787fd8db2155b748c8c0e75c58c566b58e))


### Documentation

* add new release process ([#332](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/332)) ([6015127](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/60151279e8c213ba63870de9473329a75c981754))
* Added python install commands ([#264](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/264)) ([0936d84](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0936d843c215ed8b6c81385175f23cf705278a9a))

## [1.4.0](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.3.2...v1.4.0) (2021-09-30)


### Features

* add state manager client ([#311](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/311)) ([e893ea5](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e893ea5f723e0e33350410e64e0d588cc96b36cf))
* Allow user to specify a format for stdout ([#242](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/242)) ([#293](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/293)) ([f0a9fa1](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f0a9fa1e94e86def089c77912cf49911aa63cae1))
* Allow user to specify a format for stdout T2 ([#242](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/242)) ([#296](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/296)) ([ec1af22](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ec1af22006f75b11430ac87262ae55634dc897a6))
* cast aggregates ([#306](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/306)) ([e3da4c3](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e3da4c3a9d156b864e846e8a526c4b0e864cd2e7))
* Issue262 impala connect ([#281](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/281)) ([eaa052f](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/eaa052f8b003899857fe2c8a47afaeee33e7ffd3))
* logic to deploy dvt on Cloud Run ([#280](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/280)) ([9076286](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/90762862b476d7fba531affa4f35985a51add0e4))
* promote 3.9 to main version (as it is in Cloudtops now for local testing) and add a small unit test for persoanl use ([#292](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/292)) ([eb0f21a](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/eb0f21a559a332f198c16280732a35cc3efea5b5))
* Refactor CLI to fit Command Pattern ([#303](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/303)) ([f6d2b9d](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f6d2b9d6879aaab1ab92f2f907d9334e15f75d7b))
* Updated Cloud Functions sample ([#297](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/297)) ([923413d](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/923413d30ffeed2d1dd8483758c39009b1bd9aa0))


### Bug Fixes

* updated code so that BQ target schema would not set to None for FileSystem to BQ validations ([#309](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/309)) ([5016d65](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5016d65f24bc950f8c62d248b10a73413da5d49f))

### [1.3.2](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.3.1...v1.3.2) (2021-06-29)


### Documentation

* add secrets logic to ci ([#273](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/273)) ([3c21ee5](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/3c21ee5e35bb9ad33b2406b78fa3d913dee114d4))
* Issue263 Installation doc updates ([#270](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/270)) ([0328c0e](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0328c0e4099963982a6b7ddb44b64d913379f2c1))

### [1.3.1](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.3.0...v1.3.1) (2021-06-28)


### Documentation

* clean setup ([#272](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/272)) ([08d393b](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/08d393bde323fd793e7c325f6f2c8dbeb1dc546a))
* Update docs with examples ([#261](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/261)) ([fd90096](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/fd900965a6e4fbe2f10863e154cfdd2c7d47a142))

## [1.3.0](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.2.0...v1.3.0) (2021-06-28)


### Features

* add table matching score as a param incase adjusted is needed ([#267](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/267)) ([b02aed5](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b02aed56df523fc2346116be12076661a3cff413))
* CI/CD Release to PyPi via Cloud Build ([#258](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/258)) ([0870fc7](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0870fc70cfb2115dcbead5eae6398124885cd0f4))


### Bug Fixes

* correct issues blocking impala and hive ([#266](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/266)) ([5110d1f](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5110d1fc537b87bde80bb53411129e106d8695c5))

## [1.2.0](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.1.7...v1.2.0) (2021-05-27)


### Features

* add data source for Cloud Spanner ([#206](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/206)) ([c63f68e](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c63f68edb7af1eb3abf7b2922062d477ef4f8aed))
* added an optional beta flag in CLI ([#249](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/249)) ([e8e75de](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e8e75de2443491a7007e9bda741b9821ad2f3a00))
* Added FileSystem connection type ([#254](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/254)) ([be7824d](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/be7824df8cd5bacd61862c7ff266b70b698461fd))


### Bug Fixes

* Cli tools bug fix ([#253](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/253)) ([b41e625](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b41e6251bb240e2667f25fcb05e45893f0fbe62e))
* Remove JSON arguments in CLI ([#247](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/247)) ([5a309f7](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5a309f7d4f2fc8e1ec9d55552541030c993ae306))
* Update connections.md ([#248](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/248)) ([9c1ae40](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/9c1ae400d5c2048c93222e48f336b43df2eebef2))
* Update Readme.md ([#257](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/issues/257)) ([c968024](https://www.github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c9680242e8ae41dea63bb866c5a9738810f3da15))

## 1.1.8

- Adding and documenting `find-tables` CLI feature with schema filter
- Correct filter errors caused by SQL Alchemy errors
- Adding beta calculated fields logic

## 1.1.7

- Adding tests to validate BIGNUMERIC BQ type behavior

## 1.1.6

- Minor fix for Teradata client from breaking IBis changes

## 1.1.5

- Add support for running raw queries against a connection
- Upgraded Ibis to v1.4 with large client organizational and design changes
- Added support for "use_no_lock_tables" Teradata config to optionally avoid
  table locking

## 1.1.4

- Added an options to add key:value labels to validation runs
- Oracle and SQL Alchemy now support RawSql filters
- Add support for Cloud Functions in samples
- Added schema information to result set

## 1.1.3

- Release find-tables logic too help build table lists
- Teradata client improvements
- Remove rarely used dependencies into extras

## 1.1.2

- Teradata numeric column and general bug fixes
- Fix Ibis query compliation order causing cross join

## 1.1.1

- Bug fixes to support case insensitivity
- Allow null values to be handled in grouped columns
- Oracle client improvements

## 1.1.0

- Added Row validations for cell level validation with primary keys
- Client support for Oracle, SQL Server, Postgres, and GCS files

## 1.0

- Support for Column and GroupedColumn validations
- Allow custom filter via YAML config
- BigQuery result handlers supported
- Client support for BigQuery, MySQL, and Teradata

## 0.1.1 (release date TBD)

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
