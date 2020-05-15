# Copyright 2020 Google LLC
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

""" A ResultHandler class is supplied to the DataValidation manager class.

The execute function of any result handler is used to process
the validation results.  It expects to receive the config
for the vlaidation and Pandas DataFrame with the results
from the validation run.

Output validation report to text-based log
"""


class TextResultHandler(object):
    def execute(self, config, result_df):
        print(result_df.to_string(index=False))

        return result_df
