# Goal

Create a script to create new instances of CloudSQL that are more secure and up to date for DVT testing:
1. Create new instances of CloudSQL with the latest versions of SQLServer, MySQL and Postgres; these instances should only have internal ip enabled, no external ip
2. Make a backup of the testing database (mysql=pso_data_validator, sqlserver=guestbook, postgres=guestbook) of the existing cloud sql instances
  mysql=pso-kokoro-resources:us-central1:data-validator-mysql
  postgres=pso-kokoro-resources:us-central1:data-validator-postgres12
  mssql=pso-kokoro-resources:us-central1:data-validator-mssql2017
3. Create these databases in the new instances of CloudSQL you created earlier by restoring these from the backup you just created.
4. Update CLOUD_SQL_CONNECTION in cloudbuild.yaml to point to new databases.
