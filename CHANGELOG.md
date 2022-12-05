# Changelog

## Untagged

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
