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
from google.cloud import spanner

code_to_spanner_dtype_dict = {
    1 : 'BOOL',
    2 : 'INT64',
    3 : 'FLOAT64',
    4 : 'TIMESTAMP',
    5 : 'DATE',
    6 : 'STRING',
    7 : 'BYTES',
    8 : 'ARRAY',
    10 : 'NUMERIC'
}

class pandas_df():

    def to_pandas(snapshot, sql, query_parameters):
        
        if(query_parameters):
            param={}
            param_type={}
            for i in query_parameters:
                param.update(i['params'])
                param_type.update(i['param_types'])


            data_qry=snapshot.execute_sql(sql, params=param, param_types=param_type)

        else:
            data_qry=snapshot.execute_sql(sql)

        data=[]
        for row in data_qry:
            data.append(row)
        

        columns_dict={}
        
        for item in data_qry.fields :
            columns_dict[item.name]=code_to_spanner_dtype_dict[item.type_.code]

        #Creating list of columns to be mapped with the data
        column_list=[k for k,v in columns_dict.items()]

        #Creating pandas dataframe from data and columns_list
        df = DataFrame(data,columns=column_list)


        #Mapping dictionary to map every spanner datatype to a pandas compatible datatype
        mapping_dict={
        'INT64':'int64',
        'STRING':'object',
        'BOOL':'bool',
	'BYTES':'object', 
        'ARRAY':'object',
        'DATE':'datetime64[ns, UTC]',
        'FLOAT64':'float64', 
        'NUMERIC':'object', 
        'TIMESTAMP':'datetime64[ns, UTC]'
        }
        for k,v in columns_dict.items() :
            try:
                df[k]= df[k].astype(mapping_dict[v])
            except KeyError:
                print("Spanner Datatype not present in datatype mapping dictionary")
    
        return df
