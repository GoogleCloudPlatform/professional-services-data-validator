"""
Copyright 2019 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


BigQuery Client to Manage Query Execution

"""
from . import data_client

import time
import uuid
import pandas

from datetime import datetime
from google.cloud import bigquery

def generate_state_id(string_length=5):
    """Returns a random string of length string_length."""
    random = str(uuid.uuid4())
    random = random.replace("-", "")
    return random[0:string_length]

class BigQueryClient(data_client.DataClient):

    SOURCE_TYPE = "BigQuery"
    DEFAULT_QUOTE = '`'

    def get_connection(self):
        """ Get a new connection to the client """
        print(self.config)
        project_id = self.config["project_id"]
        return bigquery.Client(project_id)

    def is_connected(self, conn):
        """ Checks if a connection is still active """
        return True

    def read_sql(self, sql):
        """ Return record results from query """
        job = self.conn.query(sql)
        self.wait_for_job(job)

        return [{key:row[key] for key in row.keys()} for row in job.result()]

    def execute_job(self, query, dataset=None, table=None, write_truncate=True, verbose=False):
        """ Return BQ Job from executed query
            
            :param query: Query to run
            :param dataset: Dataset Name if destination table
            :param table: Table Name if destination table
        """
        job_name = 'job-%s-%s-%s' % \
                   (table or "execute" ,
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'),
                    generate_state_id())
        # Prepare Job Config
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False

        # If Loading to a Destination Table
        if table:
            dataset_obj = self.conn.dataset(dataset)
            table_obj = dataset_obj.table(table)

            job_config.destination = table_obj

            job_config.flatten_results = False
            job_config.allow_large_results = True

            job_config.create_disposition = 'CREATE_IF_NEEDED'
            if write_truncate:
                job_config.write_disposition = 'WRITE_TRUNCATE'
            else:
                job_config.write_disposition = 'WRITE_APPEND'

        if verbose:
            MSG = "*** Execute Job: {job_name} ***{query}\nJob Parameters:{job_config}"
            msg = MSG.format(job_name=job_name, query=query, job_config=str(job_config._properties))
            print(msg)

        job = self.conn.query(query, job_id=job_name, job_config=job_config)
        self.wait_for_job(job)

        if job.errors:
            raise Exception(job.errors[0])

        return job

    def wait_for_job(self, job):
        while True:
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(job.errors)
                return

            try:
                job.reload()
            except exceptions.NotFound:
                # Insufficient permissions to reload (aka get) the job
                job_set = {listed_job.name for listed_job in
                           self.conn.list_jobs(state_filter='done')[0]}
                if job.name in job_set:
                    return
