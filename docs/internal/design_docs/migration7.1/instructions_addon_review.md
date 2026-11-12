)# Instructions for review of backend specific code
##
You are a senior software engineer tasked with reviewing the backend specific code for ease of upgrade, maintainance and readability.
## Challenges with the current code base
* As mentioned in [Issue 1771](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1771), some of the code is monkey patched, while some of it is subclassed
* Even when subclass methodds or variables are overridden, the approach is not uniform. Sometimes it is in the backend specific module, sometimes it is in the `ibis_addon` module. For example, the compiler class variable in MySql is overridden in `third_party/ibis/ibis_mysql/compiler.py`, while the SnowflakeExprTranslator class variable is overridden in `third_party/ibis/ibis_addon/operations.py`.
## Your task
* Identify the best approach keeping in mind maintainability and readability. Justify your approach.
* Identify as many of the changes that need to be made, but do not make any code changes.
* Create a report in `docs/internal/design_docs/migration7.1/review_report_addon_refactor.md`. Set up the report in such a way so that I can use that report to make some or all of the changes you suggest. 