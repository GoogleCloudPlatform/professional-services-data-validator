# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pandas import DataFrame


class pandas_df:
    def to_pandas(snapshot, sql, query_parameters):

        if query_parameters:
            param = {}
            param_type = {}
            for i in query_parameters:
                param.update(i["params"])
                param_type.update(i["param_types"])

            data_qry = snapshot.execute_sql(sql, params=param, param_types=param_type)

        else:
            data_qry = snapshot.execute_sql(sql)

        data = []
        for row in data_qry:
            data.append(row)

        columns = [f.name for f in data_qry.fields]

        # Creating pandas dataframe from data and columns
        df = DataFrame(data, columns=columns)

        return df
