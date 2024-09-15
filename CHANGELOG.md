# Changelog

## Untagged

## [6.1.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v6.0.0...v6.1.0) (2024-08-27)


### Features

* Add Oracle TIMESTAMP WITH LOCAL TIME ZONE support ([#1238](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1238)) ([1e9f458](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1e9f458c33ed072987779580ad3a20829ba5c7fc))
* Add TIME data type support for Teradata, BigQuery and SQL Server ([#1229](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1229)) ([ab7007b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ab7007b0ae21a6f9e388aeb20a6778ed91096e0a))
* Adding exclude_columns flag for row validation hash ([#1243](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1243)) ([a1fd616](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a1fd616e9f8b38ee464675faa629ee0dee9cdcf4))
* Auto split row concat/hash validations when many columns ([#1233](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1233)) ([ae9b72d](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ae9b72d7d2ce02dbfc72d436c702c56048255663))
* BigQuery result handler logs textual output at DEBUG instead of INFO ([#1240](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1240)) ([a9aafa2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a9aafa20406c8c3d28abbd073c7a9c4f55799bbd))


### Bug Fixes

* Adds automatic RTRIM for custom query row validations for Teradata string comparison fields ([#1230](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1230)) ([5c1a2be](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5c1a2be602658dc513d7f9e54034200de7ad8f4d))
* Bug for generate partitions with dates with different column names for PKs ([#1231](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1231)) ([5f51653](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5f51653344cdc8912ace5dfe30434a838dd4c861))
* Fixing multiple function call, to get schema in custom-query validation for Hive ([#1180](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1180)) ([a584e5b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a584e5b435f727b2463044bcfa9a7ebef5bfe951))
* support generate partitions for one row per partition ([#1241](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1241)) ([4099a29](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/4099a29e40ed0fd94e42c3e1005a1e3a50d33a0c))
* TD to BQ - Support hash row validation with Latin and Unicode Characters in Strings ([#1226](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1226)) ([e1b24ef](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e1b24efe94883ab4945908736630f283ea237d80))
* TD to BQ generate partitions with date PKs ([#1220](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1220)) ([a03f3b0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a03f3b0ede2f4b7c8918782117267fdb3d09547d))


### Documentation

* Update Cloud Functions sample to support Cloud Scheduler job triggering ([#1228](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1228)) ([3a7c164](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/3a7c1649acdc01a0982ed765db83ef53d4b307ba))

## [6.0.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v5.1.1...v6.0.0) (2024-08-06)


### ⚠ BREAKING CHANGES

* Failing validations of strings with extended ascii characters ([#1184](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1184))

### Features

* Add TLS options to PostgreSQL connections add ([#1199](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1199)) ([5506680](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/55066804a28ca8f2ade91613fc1c861354634778))
* Enhance PostgreSQL connections add documentation ([#1200](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1200)) ([f179f5a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f179f5ae9e95366ebdf77778bc2f7232662f3a52))
* generate-table-partitions write multiple partitions to a single yaml file ([#1178](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1178)) ([59a9a18](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/59a9a18b1ecd8797324768d999158654c8c5b2f2))


### Bug Fixes

* Apply RTRIM on string column when generating partitions with `-tsp` ([#1182](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1182)) ([9dcaad1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/9dcaad16e3de7032597571bac7a6f671d1686498))
* Close source and target connections after executing a validation ([#1197](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1197)) ([3dc9fa7](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/3dc9fa77677194f4b5ca99f8c1293f7e5cc042bb))
* Ensure BigQuery queries are executed in UTC ([#1174](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1174)) ([a77cdd9](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a77cdd9acd07b3f54be3b8a146461b91ab03621e))
* Ensure Teradata OutofBound dates don't affect other date columns ([#1219](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1219)) ([e02609d](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e02609d0161f7aaaa0786b1b89abbe1f08cca6c5))
* Failing validations of strings with extended ascii characters ([#1184](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1184)) ([c8ac146](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c8ac1460608ce922da8105e65205117e1bf78be4))
* Issue with GoogleSQL regarding dates before 1000CE ([#1186](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1186)) ([6107d9c](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/6107d9c480568147e1854ba4f6a88c3fe967dc5c))
* More robust __exit__ on DataValidation() ([#1206](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1206)) ([13ec3ee](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/13ec3ee6dd25aa3e60c84b2af205ff78669b7946))
* Oracle cast(decimal to string) caters for non-zero values &lt;1 ([#1204](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1204)) ([fa44466](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/fa444669e060fadc5edfd44a1f88e84bc48347bf))
* Oracle INTERVAL exception in validate schema ([#1215](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1215)) ([f133f73](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f133f732a9f9b0da6e7190a3cbc670c8c58e087a))
* Support char comparison fields in Teradata ([#1203](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1203)) ([dc19580](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/dc19580eece27123d21adeb1fac0f5e8328bc588))
* Support DATE '0001-01-01' comparison fields ([#1208](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1208)) ([cca0c4f](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/cca0c4fa702652b013b16eb2636078429e599f17))


### Documentation

* Fix some typos in Cloud Run readme ([#1188](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1188)) ([c8cd0de](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c8cd0de8795f7b2e14398fda8e3c1aca69085ec9))
* Tweaks to Cloud Functions sample ([#1176](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1176)) ([7a49a95](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7a49a958e48f99df1e1c6a2685201161d835b878))

## [5.1.1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v5.1.0...v5.1.1) (2024-06-12)


### Documentation

* Update generate-partitions flags ([#1168](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1168)) ([5a747c2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5a747c23ba5e140619feeb54ffcc7a00a6266877))

## [5.1.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v5.0.0...v5.1.0) (2024-06-11)


### Features

* Add a workaround for a Snowflake IN list limitation ([#1152](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1152)) ([16b979e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/16b979eb4977316a36d063aac6ee406698d9636d))
* Support `--trim-string-pks` flag for padded string semantics ([#1166](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1166)) ([a81f396](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a81f39673ff76dcc8f7c4f6092b95d5d92f5947f))
* Support GCS custom query files ([#1155](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1155)) ([e3fe3d1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e3fe3d184ddb4ffecddf83f5a87e681d787ea9db))


### Bug Fixes

* Fixes bug in get_max_in_list_size ([#1158](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1158)) ([973e6b6](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/973e6b64268c4ac62640a215be1a4788e61d07a1))
* Removing t0 alias from column name, while getting schema from query. Adding Integration test for Hive Custom-Query ([#1164](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1164)) ([74a14af](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/74a14af0adf0c18fc3c29124da88bd0784185d4f))
* Support PKs with different casing for generate-partitions ([#1142](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1142)) ([021ce75](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/021ce75581fd5ea85cb724132119eeeab007154a))
* Update to support up to 10K partitions ([#1139](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1139)) ([210c352](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/210c352ae8a601833a6e34b13972145abc1d49a6))

## [5.0.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v4.5.0...v5.0.0) (2024-05-21)


### ⚠ BREAKING CHANGES

* Support for GCS config paths decoupled from environment variables ([#1129](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1129))
* Filters not working correctly in Snowflake ([#1126](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1126))

### Features

* Add support for random row sampling on binary id columns ([#1135](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1135)) ([c3d2155](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c3d21557d33da4c10f4dc2b903a9c82a68adc787))
* Control Teradata decimal format when cast to string ([#1138](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1138)) ([e68e2a6](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e68e2a6965057ce57dc6957627f09a9603b718ab))
* Support for GCS config paths decoupled from environment variables ([#1129](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1129)) ([72e41b7](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/72e41b7192b95b00284500514b19d26fed48ca85))


### Bug Fixes

* Filters not working correctly in Snowflake ([#1126](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1126)) ([9845643](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/984564318b2fc6bf3f60a8a4d15078666bcf8264))
* Fix casting from binary to string on Snowflake & BigQuery ([#1113](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1113)) ([4f5ae81](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/4f5ae812420baee9b36e475c511edda4c98653ba))
* Issue 1127 configs dir fails with more than 40 files ([#1130](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1130)) ([15c81cf](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/15c81cf65e0de0cff275a5db5abf1c2fe71c2aa1))
* Teradata's ValueError after large timestamp epoch second handling ([#1121](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1121)) ([ee8d6da](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ee8d6daf3b20f03dada4d99949b11d5c62b1baf2))


### Documentation

* Add custom-query code snippet for Cloud Run sample documentation ([#1124](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1124)) ([93bb64f](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/93bb64fd45771a509104be75aceebd86b3a36d63))
* Distributed DVT Cloud Run Jobs sample ([#1133](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1133)) ([f51f327](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f51f32751bfb8713ac04de6456421a3d4a8ff1ce))

## [4.5.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v4.4.0...v4.5.0) (2024-03-18)


### Features

* Support GCS files in configs list command ([#1108](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1108)) ([b49e1c3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b49e1c373eacee6dab075cd72e6064cb46966b13))


### Bug Fixes

* Add table names to report results when source and/or target dataframes are empty ([#1104](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1104)) ([812ed62](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/812ed6274fdbc1f8847caa1d0d2013ae4acb9041))
* Fixes issue casting Snowflake decimal with scale&gt;0 to string ([#1110](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1110)) ([34446a4](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/34446a48d6a760ff57f9aa1e29796434d3e9663b))
* force cast for aggregates ([#1114](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1114)) ([44b60cf](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/44b60cfa16199a097ff7f67522b5c15541ee8f79))
* Teradata large timestamp handling ([#1117](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1117)) ([842d8b7](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/842d8b7591dd2803e97c059ae9dd3e188b225c4e))

## [4.4.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v4.3.0...v4.4.0) (2024-02-22)


### Features

* Add --url to Oracle connections add options ([#1083](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1083)) ([2f078c2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/2f078c2fed590fd5db66a2fa0e8492d247d7132f))
* Add PostgreSQL OID support ([#1076](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1076)) ([58f8fcb](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/58f8fcb56d6b6c73647a925550085fea8e7a0562))
* Add support to generate a JSON config file only for applications purposes ([#1089](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1089)) ([d463038](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/d4630386500d61cade69c2f9d9cc1fa75cd27269))
* set default oracle sql alchemy arraysize to 500 ([#1088](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1088)) ([1672ac5](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1672ac5f8c7d640af93e88f02f247f48e212892f))
* Support for Kubernetes ([#1058](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1058)) ([fdbdbe0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/fdbdbe06bd7a5a52cae63b4d701d7eb7270f20fa))


### Bug Fixes

* Add support for cx_Oracle's DB_TYPE_LONG_RAW ([#1095](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1095)) ([90547ef](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/90547efea22ae6b5b9acf9a1e3e4d2bde028dba9))
* Better casts to string for binary floats/doubles ([#1078](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1078)) ([15bfc4c](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/15bfc4c85e43894f03288cf10aa7888a95342a05))
* case-insensitive comparison field support ([#1103](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1103)) ([d28786f](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/d28786ffb9860a14f8774214baf8ed738a887165))
* Fix merge issue for Teradata empty dataframes ([#1100](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1100)) ([cc91fa2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/cc91fa296d25ae2490d00a897eb1933920a805d7))
* increase upper limit on recursion columns ([#1090](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1090)) ([c599ebf](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c599ebfdd32e24e5e3712e76961caeed76bebc95))
* Remove DDL automatically issued by Ibis for Postgres connections ([#1067](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1067)) ([c2b660b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c2b660b4f63a1fe1ca1dd258ff943f16b866e4b2))
* Row validation primary key columns &gt;64bit int/float are cast to string ([#1080](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1080)) ([9e70e9e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/9e70e9eabf96827c440776927c812ba3c64a3f97))
* Spanner generate-partition to use BQ dialect ([#1066](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1066)) ([f3cc565](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f3cc565b1cbb33e60412210bdeae986678417fcc))
* spanner hash function to return string instead of bytes ([#1062](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1062)) ([722dff9](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/722dff91a5af6cf6c9ef134117ea4dd31c568438))


### Documentation

* Add Airflow Kubernetes pod operator samples ([#1087](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1087)) ([7d5ea91](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7d5ea91163fe20268d1666d02fbe8e37f944fef3))
* Updates on nested column limitations, contributing guide examples and incorrect example ([#1082](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1082)) ([cc0f60a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/cc0f60a4921f2a37a9b376c7646978674c7a1dd2))

## [4.3.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v4.2.0...v4.3.0) (2023-11-28)


### Features

* Adding Exclude columns flag for aggregations in column validations ([#961](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/961)) ([faa32dc](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/faa32dc011fce77c12a1e2e673d671c8022c07e2))
* support query parameter for MSSQL connection ([#1026](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1026)) ([48b0355](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/48b035528df9252ef24e1baa669653da03cca6c7))


### Bug Fixes

* --dry-run for SQLAlchemy clients with valid raw SQL ([#1047](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1047)) ([c1e0e34](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c1e0e3484e33db151790b0383f9c5fa336637643))
* Add Spanner RawSQL operation to enable filtering ([#1054](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1054)) ([3a01503](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/3a015038bd5da7fdaa83e4777178e189124cde9a))
* Adding credentials as parameter for Spanner  ([#1031](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1031)) ([367658e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/367658e043204ce633c1652929bb85ab562921e9))
* Adjust `find-tables` to properly get Oracle and Postgres schemas ([#1034](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1034)) ([45fb40a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/45fb40ae9578320beceac99fb03f5d6d03ed3a76))
* Cast should treat nullable and non-nullables as the same ([#1037](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1037)) ([5e5c5eb](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5e5c5ebaa3ee27ced9654403f4b8d21fed9ca1ae))
* Fix --grouped-columns issue for Oracle validation ([#1050](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1050)) ([3473a27](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/3473a27acc0916fba0feee6e707851e5efc275b0))
* Fix decimal separator to "." (dot) on Oracle ([#1042](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1042)) ([14cc7ef](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/14cc7ef14ca5774202885638e25ac86cbe5aa4f7))
* Teradata SSLMODE issue fix ([#1014](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1014)) ([e7aab6b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e7aab6bfe5642b6725d3414d329eb688716371c6))


### Documentation

* Add CLOB to Oracle BLOB validation document ([#1029](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1029)) ([8c76c1b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/8c76c1bba214c1848d7cdcb401e5e28d3153a0a9))
* Update connections.md to add supported version of DB2 ([#1030](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1030)) ([44b4be7](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/44b4be790ca54723f0a98cb86593e55b7fade990))

## [4.2.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v4.1.0...v4.2.0) (2023-09-28)


### Features

* Add more mappings to the allowlist configuration files for Oracle schema validations ([#953](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/953)) ([0fed588](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0fed588ee89e3e8f08691675c99c428f7bb22574))
* Include date columns for min/max/sum validations ([#984](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/984)) ([6de9921](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/6de992166d3e077fe1a3fe132d758ea82e700eda))
* Include date columns in scope of wildcard_include_timestamp option ([#989](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/989)) ([a4cf773](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a4cf773a7b5302742e91306f945cb2a066a86861))
* Support BQ decimal precision and scale for schema validation ([#960](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/960)) ([b1d4942](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b1d49428d8f1990f5eee61b9e6487dbc2f561369))
* Support standard deviation for column agg ([#964](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/964)) ([bb81701](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/bb8170109d40cd8af2ffe9a6c3e3be2ea9f185c4))


### Bug Fixes

* Add exception handling for invalid value to cast a comparison field  ([#957](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/957)) ([703ca75](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/703ca7522ef94641a333312b9fb8a34a827afaf3))
* Add missing SnowflakeDialect mapping for BINARY data type ([#959](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/959)) ([9ad529a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/9ad529a73a3d53c74b69a5f5fc7e005d0e389207))
* Add not-null string to accepted date types in append_pre_agg_calc_field() ([#980](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/980)) ([76fcfc6](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/76fcfc691f07ee86d582305f52c5e83fc65664f5))
* Adjust set up for randow row batch size default value, but it maintains as 10,000 ([#986](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/986)) ([a20ccab](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a20ccabf87f77a0e91bb4d42991401b6fee5992e))
* custom query row validation failing when SQL contains upper cased columns ([#994](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/994)) ([a9fed41](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a9fed4115afa7afa823128ce4df6770169a36a2d))
* Fix warning and precision detection when target precision higher than source ([#965](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/965)) ([5f00ce1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5f00ce1b87e6b64f6e8d7a89d7f9fc542f4bc600))
* generate-table-partitions-  fixes Issue 945 and Issue 950 ([#962](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/962)) ([c53f2fc](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c53f2fc8c652daf7f806838afb2f4f2c8fcfcb0c))
* Prevent failure of column validation config generation if source column other than allow-list not present in target table. ([#974](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/974)) ([40a073e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/40a073e4ae60fe26b13f425f768cc42eed05d46a))
* Prevent Oracle blob throwing exceptions during column validation ([#1005](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1005)) ([8df1cfa](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/8df1cfaec19c62623f57dfe2a9d41240f2266cc8))
* support for case insensitive PKs and Snowflake random row ([#998](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/998)) ([1a157ae](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1a157aed71bc9ba9470be49a892f096e7dfd02f5))
* support for null columns, support for access locks ([#976](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/976)) ([f54bb4d](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f54bb4dbdabab6ac130eac3a09adbfb706086860))
* yaml validation files in gcs ([#977](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/977)) ([bf0fa0a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/bf0fa0a3d215655c15d071ee9ab8fecc93b47d68))


### Documentation

* Add a new sample code for row hash validation of Oracle BLOB ([#997](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/997)) ([0bd48a2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0bd48a2efb142795408c5079c40f83e122250325))

## [4.1.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v4.0.0...v4.1.0) (2023-08-18)


### Features

* support timestamp aggregation for Oracle and TD ([#941](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/941)) ([911bae8](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/911bae818678dc13a2b0f5a5ee7df3b7b0c75265))


### Bug Fixes

* Issues with validate column for time zoned timestamps ([#930](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/930)) ([ee7ae9a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ee7ae9a168bc7dfe3798d77ab238552c36670864))
* Schema validations ignore not null on Teradata and BigQuery ([#935](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/935)) ([936744b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/936744b37fbae8fc0718da9a515c7c1652a5dfc0))
* Support casting TD PKs to VARCHAR ([#946](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/946)) ([2171532](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/2171532a29182d4c8d95c686f72d419fc5f3ec22))

## [4.0.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v3.2.0...v4.0.0) (2023-08-02)


### ⚠ BREAKING CHANGES

* Ibis Upgrade to 5.1.0 ([#894](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/894))
* Partition based on non-numeric and multiple keys ([#889](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/889))

### Features

* Adding Random-Row support for Custom Query ([#891](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/891)) ([fc42c61](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/fc42c6181a73f79d55dc74ddd383d462a0ca4e7a))
* Adding RawSQL function for Redshift ([#903](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/903)) ([c25d690](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c25d690f3ccd41fc46768e2064d26a0754652354))
* Enhance validate schema to support time zoned timestamp columns ([#919](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/919)) ([aed1505](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/aed150530445550dcf2222dd5cb555413a9ea835))
* **generate-table-partitions:** Works on all 7 platforms - BigQuery, Hive, MySQL, Oracle, Postgres, SQL Server and Teradata. ([#922](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/922)) ([aa84d7a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/aa84d7a77a25e4f67e415d1d6fd938ebb6bfa6ce))
* Ibis Upgrade to 5.1.0 ([#894](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/894)) ([b5db4c0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b5db4c09818cc9556fedc4e14e46685a788b3919))
* Partition based on non-numeric and multiple keys ([#889](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/889)) ([7b6a530](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7b6a530bf4052eae24d2073d35d5151c24bc86df))
* Snowflake support ([#921](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/921)) ([e1d590b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e1d590bb8f0224bc316420ed42bfad0fa8065950))
* Support allow list decimals having a range for precision and scale. Also add --allow-list-file. ([#888](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/888)) ([7783beb](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7783beb347de2a4342810b50c4e65df873351b31))


### Bug Fixes

* Adding date and timestamp formatting for Hive ([#876](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/876)) ([65a090a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/65a090a99a47ed60631814c47ebbb5acf04597fb))
* Adding enhancements to allow-list in schema validation ([#881](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/881)) ([c83df2b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c83df2b165724969bee1a74989333abbed890099))
* Adding UTF encoding for Oracle hash generation ([#878](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/878)) ([2e24eae](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/2e24eae5a5dae9225bc74291ec233d32b56ea36c))
* No column filtering for csv/json text output. Reverts part of change for issue 753 ([#890](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/890)) ([ba641e0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ba641e03b49c9e7bae2fe798f5b8b2cf9e8b6c76))
* redshift bug for custom query ([#911](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/911)) ([f1018b5](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f1018b5139b8523b12e9e6440add890bcf984717))
* teradata NUMBER with no precision/scale, small doc fix after Ibis upgrade ([#914](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/914)) ([f9db68f](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f9db68f2edcc2980085945a6a65151b9428b4f48))
* validate column sum/min/max issue for decimals with precision beyond int64/float64  ([#918](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/918)) ([5a8d691](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5a8d6919cf41a1b3457993fcf7fc6ae1df8e6fd8))


### Documentation

* Add sample shell script and documentation to execute validations at a BigQuery dataset level ([#910](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/910)) ([a84da45](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a84da450468d0abcae09b081fa208f26c9677da7))

## [3.2.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v3.1.0...v3.2.0) (2023-05-31)


### Features

* Add --dry-run option to validate. ([#778](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/778)) ([8989350](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/89893505e420e4c8feefa1fbf61a35259969dd76))
* Add Impala flags for http_transport and http_path ([#829](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/829)) ([d966b9e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/d966b9e9bde36925dfabf9ce85b91bad6ba52a7d))
* Add support for SQL Server's IMAGE, BINARY, VARBINARY, NCHAR, NTEXT, NVARCHAR data types ([#859](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/859)) ([6ebece3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/6ebece37bb5e2c7eb3a5408ef10545672db2051e))
* Add support for SQL Server's MONEY data type  ([#837](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/837)) ([0749c9e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0749c9e0ea36df06226267670bc08642163c9cd8))
* Move source credentials to secret manager ([#824](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/824)) ([1dd5fea](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1dd5fea0bdca68b69989f5ed58379845bd6dbd98))
* Redshift integration for Normal row and Custom-Query Validation. ([#817](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/817)) ([92ab215](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/92ab215670a0d46a845dcd1e057346de8fb9cce0))


### Bug Fixes

* Add missing operations for SQL Server - ExtractEpochSeconds, ExtractDayOfYear, ExtractWeekOfYear ([#870](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/870)) ([709dd4c](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/709dd4cbf17e073cb83cbdde50bc1bcdaf94d4cb))
* Adding datetime and timestamp format logic ([#840](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/840)) ([eb095c9](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/eb095c9bfd9addd617a9e260e3bec5e0273695a7))
* dry-run bug when running configs, added CODEOWNERS, and docs ([#865](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/865)) ([1779772](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/177977207aa75318a231889e958e78b43510a4a5))
* handle numeric datatype mapping in teradata schema and fix int mapping as per teradata doc ([#874](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/874)) ([333eadb](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/333eadbf9136a78e51822fcae5cd22f4490bdd6e))
* split connection names from second last period instead of first from front ([#864](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/864)) ([1462deb](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1462debeffb1944ecb5580cdf2f55a49b5bf7982))
* Support for sum/min/max included for oracle number greater than int64 ([#809](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/809)) ([73bda66](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/73bda661ee9fde18486a9c5209806be3d1de7ea0))


### Documentation

* Fix typos on README ([#801](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/801)) ([14ddcc5](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/14ddcc5c55c39dbcd64d234f4f4186caeab85ab7))
* update installation guide about Python 3.11 ([#815](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/815)) ([88cd281](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/88cd28158a852a4f1713f91890b4d898b4ce2191))
* Update our documentation about `find-tables` command and the `score-cutoff` parameter ([#846](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/846)) ([54403e3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/54403e3014ff5dbcc50ca540eb786dfc33759f0b))

## [3.1.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v3.0.0...v3.1.0) (2023-04-21)


### Features

* add db2 hash and concat support ([#800](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/800)) ([c16e2f7](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c16e2f79c974412f2407ddbd0846a83b8c2cb330))
* add Impala connection optional parameters ([#743](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/743)) ([#790](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/790)) ([414d7f8](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/414d7f83bc65be2f17dd6103b7667409ff9b0269))
* added source_type in output  while listing connections list ([#803](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/803)) ([056275b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/056275b1a78c04f7aa5e5cfa4911d35ac9af1c5a))
* Adding Custom-Query support for DB2.  ([#807](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/807)) ([a8085d3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a8085d318c7fdd4b4591c45dc8eb6f399cabb670))
* Option for simpler report output grid ([#802](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/802)) ([b92eb91](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b92eb91feb3b4a83c6ad74c4b9c5af65dcbc21f8))


### Bug Fixes

* Mysql fix to support row hash validations, random row validation, and filter ([#812](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/812)) ([ae07fa4](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ae07fa429e7c94991c570231b46a54423b2e56e5))
* schema validation fixes for Oracle/SQL Server float64 and SQL Server datetimeoffset ([#796](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/796)) ([ad0e64f](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ad0e64f4356394826fabd73d1a279ed448cb8273))


### Documentation

* add README for Airflow DAG sample, update code formatting in other docs ([#722](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/722)) ([f4c3241](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f4c3241357451947687b16eb51bee8702bbdf7d8))
* score-cutoff changed to 1 ([#779](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/779)) ([d3aabca](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/d3aabca19945564711c256972236e23c05212160))

## [3.0.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.9.0...v3.0.0) (2023-03-28)


### ⚠ BREAKING CHANGES

* issue673 optimize CLI tools arg parser ([#701](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/701))

### Features

* :sparkles: Add support for source/target inline sql queries for `validate custom-query` command ([#734](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/734)) ([c5e7a37](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c5e7a37780e8c87168938d7d60592bfe5c4b2147))
* gcp secret manger support for DVT  ([#704](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/704)) ([d6c40f1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/d6c40f1f780f634c9b31a5a6ae9e0fc9312eb5ee))
* ibis_bigquery strftime support for DATETIME columns ([#737](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/737)) ([b1141de](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b1141de481216f2adfe4cabbf3cb69efac1d5c89))


### Bug Fixes

* Add support for numeric and precision with length and precision in Postgres Custom Query ([#723](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/723)) ([742b77e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/742b77ef18741e9eee52d71f954b1400395fe5c8))
* Adding Decimal datatype support for MSSQL custom query validation ([#771](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/771)) ([0d5c5eb](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0d5c5eb88a9979a290dbbe6a2d84caecbb04f39c))
* Better detection of Oracle client ([#736](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/736)) ([efce0b8](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/efce0b838d13441e0a5940f394d5be73cfa52fce))
* Cater for query driven comparisons in date format override code ([#733](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/733)) ([0a22643](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0a226436605de93af36894cf6497f9d7dcb337c0))
* issue 740 teradata strftime function ([#747](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/747)) ([9fd102a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/9fd102a98de5bf0e2ac824c3f2b29f287ab4709d))
* issue673 optimize CLI tools arg parser ([#701](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/701)) ([26bb8e9](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/26bb8e9f9a5301361f632a9eb1dd13aaf67628e0))
* Protect column and row validation calculated column names from Oracle 30 character identifier limit ([#749](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/749)) ([89413c1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/89413c19196d4f93c94c6363895c386ed826438f))
* remove secret manager warnings  ([#781](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/781)) ([7e72bfd](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7e72bfde4adbe07c6f7ad99c104e074652a84bed))


### Documentation

* formatting fixes and fix broken link ([#739](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/739)) ([7306dfc](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7306dfc95f3f3816703a5f31da7bc2e8f0ec21da))
* oracleuserpriv ([#746](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/746)) ([a7889bf](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a7889bf7c8b5106a4144a73a0767c2b59b4c09b0))

## [2.9.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.8.0...v2.9.0) (2023-02-16)


### Features

* Added Partition support to generate multiple YAML config files ([#653](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/653)) (Issue [#619](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/619),[#662](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/662)) ([f79c308](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f79c30873e9ec7a0377cdbce4e781cdf69a2a305))
* added run_id to output  ([#708](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/708)) ([17720f2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/17720f2b9abde1ea66e39ab26d084bfa1b77b54a))
* Divert cast of PostgreSQL decimal with scale&gt;0 to to_char ([#721](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/721)) ([3542851](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/354285112236721ffe4071d690da1cbdd99d9bba))
* Use centralized date/time format in order to compare row data across engines ([#720](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/720)) ([0de823b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0de823b4298f8739d656f7069e300c7fdaf08b7a))


### Bug Fixes

* Error handling for batch processing of config files ([#663](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/663)) ([21a26af](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/21a26afade76c1f9143605d672b552f2d4d54115))
* Protect non-date columns from astype(str) date workaround ([#726](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/726)) ([489ee27](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/489ee2762aed320278f6a2ebdd430b7717dc39d2))
* schema validation fix for different base names of source and destination data types ([#710](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/710)) ([d7b44b0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/d7b44b03a06dc1f9f0b6342051510f058464b3c2))


### Documentation

* updated Oracle parameter from user_name to user and changed underscores to hypens across the document ([#689](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/689)) ([8777e00](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/8777e00d048970b6c833825a23de335b4a87c249))

## [2.8.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.7.0...v2.8.0) (2023-01-19)


### Features

* Logic to add allow-list to support datatype matching with a provided list in case of mismatched datatypes between source and target ([#643](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/643)) ([269f8dc](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/269f8dc7d8afa78fe8ecc8c79b0eb3d197d1e8f0))


### Bug Fixes

* making logmech as optional for TD connection ([#665](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/665)) ([500caa3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/500caa33aa5a8983277e73c39bc0733dd684161b))

## [2.7.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.6.0...v2.7.0) (2023-01-06)


### Features

* Add AlloyDB support ([#645](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/645)) ([cfedc22](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/cfedc224a21633046ff7551070c88cd79f7e754e))
* Add Integration test for Oracle ([#651](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/651)) ([de3bbcc](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/de3bbccb6dea5b83f2106692da9de6b4b89310dd))
* Added custom query support for Oracle ([#646](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/646)) ([3f8771a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/3f8771a8969b393324b5f831573ba7436f819eb8))
* Added custom query support for PostgreSQL ([#644](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/644)) ([88dcfd3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/88dcfd3690c789d09bef3665d85ecf518fd6fa47))
* extend TO_CHAR to cover date, time and timestamp types ([#641](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/641)) ([e0c184f](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e0c184fadf91c396c4815b0de2f6e0359643deb2))
* SQL Server custom query support ([#640](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/640)) ([98ab010](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/98ab01028f3d4e3a62f410a44ff9b315d3e6228e))
* Support config directory for running validations and add multithreading for DB queries ([#654](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/654)) ([c67b51a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c67b51a9e1af570339a0457a549d540680984c5e))
* Support custom calculated fields ([#637](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/637)) ([14b506b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/14b506b34ef23fce6764ad62e763b15aac881ccc))

## [2.6.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.5.0...v2.6.0) (2022-11-28)


### Features

* add random row support for MSSQL ([#633](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/633)) ([3041bd1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/3041bd142ca48860a1861486dd10cdd791b7f8bf))
* to_char support for Oracle and Postgres ([#632](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/632)) ([78f1ce9](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/78f1ce9acc1e956106540a492c8d442485cd7f4c))


### Bug Fixes

* bare data-validation command throws exception ([#627](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/627)) ([7595c50](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7595c504cffd0609fd278c2889d71f5655d77592))
* column validation casing to allow for case-insensitive match ([#626](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/626)) ([c694357](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c69435794571fe639ea215f76f496b665a928419))

## [2.5.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.4.0...v2.5.0) (2022-10-18)


### Features

* adding scaffold for concatenate as a cli operation ([#566](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/566)) ([ec4ef33](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ec4ef33fe68051d6a00bb0eba227296a57887a6f))


### Bug Fixes

* Custom query validation throwing error with sql files ending with semicolon(;) ([#591](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/591)) ([16a89ac](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/16a89ac64489b9bff16407c6d31b6b220e24bb06))
* Row validation optimization to avoid select all columns ([#599](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/599)) ([de3758e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/de3758ef4b40eb6c443658546d5c8f5843e58b75))
* update function to return non-unicode string ([#615](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/615)) ([e334c65](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e334c6544a78fa384381c4e306b6b7fcb2b2eb0d))

## [2.4.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.3.0...v2.4.0) (2022-10-06)


### ⚠ BREAKING CHANGES

* Add Python 3.10 support (#564)

### Features

* Add Python 3.10 support ([#564](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/564)) ([38284a5](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/38284a5ed3928a21b9889c0d57f9461e98504a68))
* New flag to filter results by status in all supported validations ([#593](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/593)) ([97e8bb0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/97e8bb056fdb89078d67d7a84adc460a821a9455))
* Oracle random row ([#588](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/588)) ([ac3460a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ac3460af84d50c3611ab27b2861bec4c0b56e39b))
* Postgres row hash validation support ([#589](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/589)) ([01765b3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/01765b31e507e80fc6f4db579b18ec9a8ea608f3))


### Miscellaneous Chores

* release 2.4.0 ([#600](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/600)) ([b704505](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b704505842e15625d7dc181ef7f946ac82633836))

## [2.3.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.2.0...v2.3.0) (2022-09-15)


### Features

* Addition of log level as an argument for DVT logging and replac… ([#577](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/577)) ([dbd9bc3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/dbd9bc387778580a106b39f4dfb9c10fd63d2f6f))
* Oracle row level validation support ([#583](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/583)) ([489654c](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/489654ca5848b34be780a3535ecd0063075f8c97))


### Bug Fixes

* Add RawSQL support for Postgres and SQL Server ([#576](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/576)) ([0693782](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/069378271fdab08af8bfe984590b63a45c199bbb))
* fixing String to varchar for teradata ([a979931](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a979931787bdaa04ac0f52ee2a8946eca54e8a8e))
* random rows with filter option ([#582](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/582)) ([da4faaf](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/da4faaf12ae65aa81310c872757d6ce4d7518106))
* support NUMBER with no precision/scale ([#572](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/572)) ([03219ba](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/03219ba6d6b4e30bf24c62b57b916ff52bb85ee6))
* Teradata limit on column name, bug when casting to VARCHAR ([#580](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/580)) ([c8700be](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c8700be79315ecf8efd1c14ab7a17d236ef53adb))


### Documentation

* remove snowflake, add row supported DBs ([#587](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/587)) ([1d923f5](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1d923f54b64a30219cb9d33533ac46f9121ecf60))

## [2.2.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.1.0...v2.2.0) (2022-08-29)


### ⚠ BREAKING CHANGES

* Added teradata custom query support (#547)

### Features

* Added teradata custom query support ([#547](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/547)) ([97c3203](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/97c320341dc6d17fc390fcd2dc503ab2411cab57))
* Improve schema validation debugging, Support DATE for Hive validations ([#558](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/558)) ([e67de5b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e67de5b20623c59ce04d01e9f5acc8ea293809e1))
* Support for MSSQL row validation ([#570](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/570)) ([61dabe0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/61dabe0f96192f065c67ba24af9d6764e21c53f7))


### Bug Fixes

* Issue422 replace print with logging ([#543](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/543)) ([78222b4](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/78222b4d3780818112426f9e6f486301b5b9786e))


### Miscellaneous Chores

* release 2.2.0 ([#571](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/571)) ([c29b4c1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c29b4c1d9fb7d2d80079832bc139b8faa3f05826))

## [2.1.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.0.1...v2.1.0) (2022-07-14)


### Features

* new flag to exclude columns from schema validation ([#507](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/507)) ([53ac41a](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/53ac41a5df368cadd47298e6484d7e82823b0fbc))
* Remove dependency on tables list for custom query ([#541](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/541)) ([7dca5bd](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7dca5bd9ca3d2701ef33e5b1a7925adc36bfd6d2))


### Bug Fixes

* added new result columns to schema validation ([#512](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/512)) ([478bb2d](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/478bb2d099325c5bf49f44a4aa84f7b1a17123ac))
* close Teradata connection via object delete ([#524](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/524)) ([181b865](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/181b8654d05da3fcea78cfadd978a28282f8736b))
* editing contributing.md ([#509](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/509)) ([c01d730](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c01d7301129b9e42153419c85a64d59ce2f46c2b))
* fixing teradata doc ([#513](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/513)) ([6a10356](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/6a103564da6f100f9aa601089c0ab1d5962663e3))
* issue-256-bug fixes to generate docker file ([#531](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/531)) ([adc528e](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/adc528e999376cd088d833cd5f8465bed27b7acb))
* issue-256-Release docker image for dvt repo ([#527](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/527)) ([e3d42cc](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e3d42cc9a1dea092d15687e9995fa41f18f59aeb))
* issue-256-Release docker image for dvt repo ([#529](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/529)) ([e87d0ef](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e87d0ef54b1d3752ef3540aa5428368a778f70e5))
* Oracle support for decimals ([#530](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/530)) ([0d73207](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/0d73207bc6ddd81d94bad62251cadb52d49e64a3))
* primary key casting ([#521](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/521)) ([1a7667b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/1a7667b4196b393f0d29b62d2123b082edebd3e2))
* support for cast to timestamp in TD, support for random row ([#538](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/538)) ([f7ed739](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f7ed739008a59e66ac8ba686ddfd7c6afced855a))


### Documentation

* fix typo on ibis_snowflake ([#516](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/516)) ([de8a4bd](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/de8a4bd6b852dc8e01e3455b65179c75c4e6d91f))
* supported hive version ([#515](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/515)) ([923d4ff](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/923d4ffd05c436a11784f6d41918000fb877d2b1))

## [2.0.1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v2.0.0...v2.0.1) (2022-06-10)


### Bug Fixes

* Schema validation to make case insensitive column name comparision ([#500](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/500)) ([ee8c542](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ee8c54217ecc6b739fc9f9ae6c237eb3acfe46a0))

## [2.0.0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.7.2...v2.0.0) (2022-05-26)


### ⚠ BREAKING CHANGES

* Add 'primary_keys' and 'num_random_rows' fields to result handler (#372)

### Features

* Add 'primary_keys' and 'num_random_rows' fields to result handler ([#372](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/372)) ([b123279](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b1232791f89fb39491a65cd1945f272d85e521b1))
* add a new DAG example to run DVT ([#485](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/485)) ([e3dd7ed](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e3dd7ed1d524c613f9c78c36bb1ad5346c37ec62))
* adding impala random function ([#483](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/483)) ([93d2072](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/93d2072fda050fd35505db3f04907c664e30f18c))
* Enable sum/avg/bit_xor for BigQuery datetime type ([#488](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/488)) ([083de07](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/083de076d32e2203531de3feffb7174df6a05908))


### Documentation

* Alpha-order Connection Types ([#491](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/491)) ([39e0dd8](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/39e0dd8d641347b7c4852c3784ae81b94c23f99f))
* GA README updates ([#492](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/492)) ([b63ef3b](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/b63ef3b6240ea4bd550db5cf635fdcdac8bb7634))

### [1.7.2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.7.1...v1.7.2) (2022-05-12)


### ⚠ BREAKING CHANGES

* Adds custom query row level hash validation feature. (#440)

### Features

* Add example of BigQuery cast to NUMERIC, update chore release version ([#476](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/476)) ([50fac28](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/50fac2855be4b3ca0607dd6a777699a778c56d14))
* Adds custom query row level hash validation feature. ([#440](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/440)) ([f057fe8](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f057fe8d690c78219f6341d210ba9719d4510fd6))
* Issue356 db2 test ([#383](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/383)) ([70fb7bc](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/70fb7bc07517665eff554b341a678e00d458a2b1))
* Support cast to BIGINT before aggregation ([#461](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/461)) ([ca598a0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/ca598a0a1ca80541f98b5108d3fd358081af2c0b))
* support float and decimal types in Hive ([#470](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/470)) ([5936f60](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/5936f6046d1eef0094b0e225cd05dc4f9e150c93))


### Bug Fixes

* add get_ibis_table_schema ([#410](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/410)) ([#411](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/411)) ([4093625](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/409362510d7e405016e87e253e4127a04089fabd))
* only replaces datatypes and not column names ([#453](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/453)) ([6143794](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/6143794e90e048243f88ee811c6de5cf84a70184))
* supports NULL datetime/timestamps, fixes bug with validation_status in PR 455 ([#460](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/460)) ([57896f4](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/57896f430967f9726ccdf8c82b1c3b9833f0062c))
* Updated schema validation logic to column as 'validation_status' ([#455](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/455)) ([e30c337](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e30c33750edc4a128e72ff2940106d4404cab0be))
* updating teradata docs for sha256 UDF and swapping string_join for concat ([#457](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/457)) ([23dbf56](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/23dbf566dae054c1130c5192fb1a1cd7b20de501))

### [1.7.1](https://github.com/GoogleCloudPlatform/professional-services-data-validator/compare/v1.7.0...v1.7.1) (2022-04-14)


### ⚠ BREAKING CHANGES

* Changed result schema 'status' column to 'validation_status' (#420)

### Features

* added timestamp to supported types for min and max ([#431](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/431)) ([e8b4860](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e8b48603ff851d771c01794262ab1281192dea0e))
* Allow aggregation over length of string columns ([#430](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/430)) ([201f0a2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/201f0a2a67e79190400aba668fc34df54bf87b66))
* Changed result schema 'status' column to 'validation_status' ([#420](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/420)) ([dfcd0d5](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/dfcd0d5000fb33e64a8580a0747319f5ddbec2ca))
* hash filter support ([#408](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/408)) ([46b3723](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/46b3723390bf050ae33868517478906d63a43304))
* Hash selective columns ([#407](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/407)) ([88b6620](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/88b66201750db3e1a0577f12965551f8955a2525))
* Implement sum/avg/bit_xor aggs for Timestamp ([#442](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/442)) ([51f3af3](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/51f3af3c6bb11beaf7bc08e6b2766e0b5a6b8600))
* improve postgres tests ([#443](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/443)) ([6a54527](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/6a54527b758c56cc1aa0a1a15b355970115fa16f))
* Random Sort for Pandas Queries ([#404](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/404)) ([2051039](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/20510395193e831d8e3350842ce606f4adb80fcb))
* Support for custom query ([#390](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/390)) ([7a218d2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7a218d2b516d480dad05f6fb52ed6347aef429ed))


### Bug Fixes

* bug introduced with new pr ([#429](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/429)) ([a6cf3f0](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/a6cf3f03e8f28eb6b3c2cfca41880d839f301637))
* Hash all bug, noxfile updates ([#413](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/413)) ([fc73e21](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/fc73e21810ffe74459a90b79b956a91168c0dc1c))
* Hive boolean nan to None, Unsupported ibis data types in structs and arrays ([#444](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/444)) ([e94a1da](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/e94a1daabf6c5df04720e20b764a8ccf9bc63050))
* ibis default sql option limits query results at 10k rows ([#418](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/418)) ([7539efe](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/7539efe266ecea22102d775dc9ad4c8bbb9dba84))
* Impala strings/objects now return None instead of NaN ([#406](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/406)) ([9d3c5ec](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/9d3c5ecf1babae2c811a30d0820701b124ae1c50))
* issue 265 add cloud spanner functionality ([#394](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/394)) ([783cdf8](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/783cdf8810c29755b26e4894555b6dd03f4c9025))
* support labels for schema validation ([#260](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/260)) ([#381](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/381)) ([f787701](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/f787701dcb505fbced3e12b996c845148bbc1af0))
* Treat both source and target values being NULL as a success ([#437](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/437)) ([c4da5ca](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/c4da5ca18f47af3e5ebadce97d35c25ca66d4003))


### Miscellaneous Chores

* release 1.7.1 ([#446](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/446)) ([99916ba](https://github.com/GoogleCloudPlatform/professional-services-data-validator/commit/99916ba2c76b8370cabd648e4d1f3c4ec15b93d7))

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
