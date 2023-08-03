# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

def null_not_null_assertions(df):
    """Standard assertions for null_not_null integration test.
    These tests use BigQuery as a set target with a mismatch of not null/nullable settings.
    All other engines are validated against BigQuery to check we get the correct status."""
    # Should be 4 columns in the Dataframe.
    assert len(df) == 4
    match_columns = ['col_nn', 'col_nullable']
    mismatch_columns = ['col_src_nn_trg_n', 'col_src_n_trg_nn']
    for column_name, status in zip(df['source_column_name'], df['validation_status']):
        assert column_name in (match_columns + mismatch_columns)
        if column_name in match_columns:
            # These columns are the same for all engines and should succeed.
            assert status == 'success', f"Column: {column_name}, status: {status} != 'success'"
        elif column_name in mismatch_columns:
            # These columns are the different for source and target engines and should fail.
            assert status == 'fail', f"Column: {column_name}, status: {status} != 'fail'"
