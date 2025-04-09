# Result handlers

Capturing some testing and thoughts around result handlers that came from implementing the
PostgreSQL result handler.

## Timings

Timings from the sections that follow this are summarised in the table below:

| Destination | Technique | Total Elapsed | RH write |
| :---- | :---- | ----- | ----- |
| Local filesystem | stdout redirect to /tmp | 43s |  |
| BigQuery | insert\_rows\_from\_dataframe | 3m24s | 2m58s |
| PostgreSQL | Ibis client.insert | 2m35s | 2m9s |
| PostgreSQL | to\_sql(chunksize=1000) | 2m12s | 1m46s |
| PostgreSQL | to\_sql(multi, chunksize=1000) | 5m48s | 5m22s |
| PostgreSQL | COPY | 50s | 24s |

## Tests

The tests below are all using a table containing 1 million rows. The validation has hash and
all 1 million validation results were persisted.

Tests were executed on the `develop` branch on 2025-04-03.

### Local filsystem

Simply redirecting results to a CSV file. This is the baseline because no data is written to
a result handler, although the CSV output is streamed to stdout which does take time.

Command:
```console
time data-validation -ll DEBUG validate row -sc=oravol -tc=pg \
-tbls=dvt_test.tab_vol_1m -pk=id --hash='*' \
--format=csv 2>/tmp/debug.log >/tmp/results.csv

real	0m43.551s
user	0m38.294s
sys	0m3.265s
```

DVT stages:
```
04/03/2025 11:34:54 AM-DEBUG: Build config elapsed: 0.17s
04/03/2025 11:34:58 AM-DEBUG: Target query elapsed: 4.3s
04/03/2025 11:35:01 AM-DEBUG: Source query elapsed: 7.65s
04/03/2025 11:35:17 AM-DEBUG: Generate report elapsed: 15.92s
```

`vmstat` output:
```
procs -----------memory---------- ---swap-- -----io---- -system-- ------cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 32084396  21824 557608    0    0    17     7  353  483  8  1 91  0  0
 0  0      0 31446832  21832 557644    0    0     3     6 4817 5293 13  3 84  0  0
 1  0      0 30715072  21840 557868    0    0    22    42 1378 2025 20  4 76  0  0
 1  0      0 30671736  21840 557880    0    0     1     0 1133 1554 24  2 75  0  0
 1  0      0 30670996  21856 557880    0    0     0     4 1126 1541 25  0 75  0  0
 0  0      0 31727748  21864 913108    0    0     0  5326 1043 1542 14  2 84  0  0
 1  0      0 31727504  21900 913108    0    0     0 29241 1002 1548  0  0 96  3  0
```

### BigQuery

Writing to BigQuery using the BigQuery client `insert_rows_from_dataframe()` method:
```python
        table = self._bigquery_client.get_table(self._table_id)
        chunk_errors = self._bigquery_client.insert_rows_from_dataframe(
            table, result_df
        )
```

Command:
```console
time data-validation -ll DEBUG validate row -sc=oravol -tc=pg \
-tbls=dvt_test.tab_vol_1m -pk=id --hash='*' \
--result-handler=<GCP_PROJECT_ID>.pso_data_validator_results.results \
2>/tmp/debug.log

real	3m24.076s
user	1m57.846s
sys	0m3.508s
```

DVT stages:
```
04/03/2025 11:12:56 AM-DEBUG: Build config elapsed: 0.18s
04/03/2025 11:13:00 AM-DEBUG: Target query elapsed: 4.44s
04/03/2025 11:13:03 AM-DEBUG: Source query elapsed: 7.59s
04/03/2025 11:13:19 AM-DEBUG: Generate report elapsed: 15.8s
04/03/2025 11:16:18 AM-DEBUG: Write results to BigQuery elapsed: 178.15s
```

`vmstat` output:
```
procs -----------memory---------- ---swap-- -----io---- -system-- ------cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 32095248  21256 557508    0    0    21     8  375  500  9  1 90  0  0
 1  0      0 31426872  21264 557512    0    0     0    34 5278 5836 14  4 82  0  0
 1  0      0 30798496  21272 557512    0    0     0     1 1352 1849 21  4 75  0  0
 1  0      0 30601272  21272 557516    0    0     0     1 1261 1751 20  1 79  0  0
 1  0      0 30606352  21288 557544    0    0     0    12 1288 1723 14  0 86  0  0
 1  0      0 30608560  21304 557564    0    0     0     2 1309 1728 14  0 86  0  0
 0  0      0 30607392  21320 557588    0    0     0     8 1277 1751 13  0 87  0  0
 1  0      0 30606060  21336 557612    0    0     0     4 1237 1583 14  0 86  0  0
 1  0      0 30606124  21352 557668    0    0     3     4 1261 1573 14  0 85  0  0
 2  0      0 30606124  21368 557692    0    0     0    11 1241 1571 14  0 86  0  0
 1  0      0 30606092  21384 557712    0    0     0     5 1220 1571 14  0 86  0  0
 1  0      0 30605964  21400 557744    0    0     0    16 1190 1558 13  0 87  0  0
 0  0      0 30605932  21416 557756    0    0     0    11 1210 1566 14  0 86  0  0
 1  0      0 30606056  21432 557792    0    0     0     3 1221 1556 14  0 86  0  0
 0  0      0 30605048  21448 557824    0    0     0    16 1233 1584 14  0 86  0  0
 1  0      0 30604292  21464 557844    0    0     0     3 1204 1596 13  0 87  0  0
 1  0      0 30604292  21480 557868    0    0     0     2 1216 1569 14  0 86  0  0
 3  0      0 30603976  21504 557896    0    0     0    34 1226 1559 14  0 86  0  0
 1  0      0 30603880  21520 557924    0    0     0     2 1232 1567 14  0 86  0  0
 0  0      0 30603816  21536 557948    0    0     0     2 1194 1561 13  0 87  0  0
 1  0      0 30604352  21556 557968    0    0     0    14 1201 1558 13  0 87  0  0
 0  0      0 32091888  21572 557980    0    0     0     4 1108 1599  9  0 90  0  0
 0  0      0 32091888  21572 557980    0    0     0     0  853 1527  0  0 100  0  0
```

### PostgreSQL - Ibis client.insert

Writing to PostgreSQL using the `insert()` method provided by Ibis.
```python
self._client.insert(table_name, result_df)
```

Command (where `pg_rh` is the name of a PostgreSQL connection configuration file):
```console
time data-validation -ll DEBUG validate row -sc=oravol -tc=pg \
-tbls=dvt_test.tab_vol_1m -pk=id --hash='*' \
--result-handler=pg_rh.pso_data_validator_results.results \
2>/tmp/debug.log

real	2m35.404s
user	1m11.604s
sys	0m3.503s
```

DVT stages:
```
04/03/2025 11:25:25 AM-DEBUG: Build config elapsed: 0.17s
04/03/2025 11:25:29 AM-DEBUG: Target query elapsed: 4.55s
04/03/2025 11:25:32 AM-DEBUG: Source query elapsed: 7.6s
04/03/2025 11:25:48 AM-DEBUG: Generate report elapsed: 15.8s
04/03/2025 11:27:58 AM-DEBUG: Write results to PostgreSQL elapsed: 129.41s
```

`vmstat` output:
```
procs -----------memory---------- ---swap-- -----io---- -system-- ------cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 32094052  21720 558024    0    0    18     7  362  489  8  1 91  0  0
 0  0      0 31423908  21728 557608    0    0     0    14 5331 5680 13  3 83  0  0
 1  0      0 30860328  21736 557608    0    0     0     6 1450 1929 21  4 75  0  0
 1  0      0 30537996  21736 557608    0    0     0     0 1156 1581 24  1 75  0  0
 1  0      0 30316580  21744 557608    0    0     0     2 1196 1658 25  0 75  0  0
 1  0      0 29349932  21752 557608    0    0     0     3 1206 1687 24  1 75  0  0
 1  0      0 28944016  21752 557608    0    0     0     0 1192 1759 10  1 89  0  0
 0  0      0 28942720  21760 557608    0    0     0     2 1160 1746  6  0 93  0  0
 0  0      0 28942464  21760 557608    0    0     0     0 1092 1611  6  0 93  0  0
 0  0      0 28941800  21760 557608    0    0     0    14 1126 1671  6  0 94  0  0
 0  0      0 28941800  21760 557608    0    0     0     2 1197 1751  6  0 93  0  0
 0  0      0 28941768  21760 557608    0    0     0     0 1112 1590  7  0 93  0  0
 1  0      0 28941860  21760 557608    0    0     0     0 1100 1594  6  0 94  0  0
 0  0      0 28941796  21760 557608    0    0     0     0 1091 1564  6  0 93  0  0
 1  0      0 28941732  21760 557608    0    0     0     0 1087 1548  7  0 93  0  0
 0  0      0 28941536  21760 557608    0    0     0     0 1076 1554  6  0 94  0  0
 0  0      0 32083492  21760 557608    0    0     0     0 1013 1544  7  0 93  0  0
 0  0      0 32083500  21768 557608    0    0     0     1  851 1525  0  0 100  0  0
```

### PostgreSQL - Dataframe.to_sql(chunksize=1000)

Writing to PostgreSQL using the pandas `to_sql(chunksize=1000)` method.
```python
            result_df.to_sql(
                table_name,
                self._client.con,
                schema=schema_name,
                if_exists="append",
                index=False,
                chunksize=1000,
            )
```

Command (where `pg_rh` is the name of a PostgreSQL connection configuration file):
```console
time data-validation -ll DEBUG validate row -sc=oravol -tc=pg \
-tbls=dvt_test.tab_vol_1m -pk=id --hash='*' \
--result-handler=pg_rh.pso_data_validator_results.results \
2>/tmp/debug.log

real	2m12.332s
user	0m53.855s
sys	0m3.094s
```

DVT stages:
```
04/03/2025 12:13:09 PM-DEBUG: Build config elapsed: 0.17s
04/03/2025 12:13:13 PM-DEBUG: Target query elapsed: 4.4s
04/03/2025 12:13:16 PM-DEBUG: Source query elapsed: 7.68s
04/03/2025 12:13:32 PM-DEBUG: Generate report elapsed: 15.9s
04/03/2025 12:15:19 PM-DEBUG: Write results to PostgreSQL elapsed: 106.13s
```

`vmstat` output:
```
procs -----------memory---------- ---swap-- -----io---- -system-- ------cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 31727084  22580 913916    0    0    13    17  337  469  8  1 91  0  0
 1  0      0 31136628  22596 913916    0    0     0    12 4592 5750 14  3 83  0  0
 1  0      0 30500128  22604 913916    0    0     0     1 1291 1798 21  4 75  0  0
 1  0      0 30169196  22604 913916    0    0     0    10 1240 1754 22  1 77  0  0
 0  0      0 30170860  22620 913916    0    0     0     5 1169 1750  8  0 92  0  0
 1  0      0 30172588  22620 913916    0    0     0     0 1160 1748  7  0 92  0  0
 0  0      0 30173500  22628 913908    0    0     0     2 1176 1750  8  0 92  0  0
 0  0      0 30175160  22628 913916    0    0     0     4 1147 1726  7  0 93  0  0
 0  0      0 30177188  22628 913916    0    0     0     0 1060 1563  6  0 94  0  0
 0  0      0 30177028  22628 913916    0    0     0    14 1115 1555  8  0 92  0  0
 0  0      0 30176932  22628 913916    0    0     0     0 1101 1551  8  0 92  0  0
 0  0      0 30176932  22628 913916    0    0     0     0 1088 1545  7  0 92  0  0
 1  0      0 30177024  22628 913916    0    0     0     0 1109 1545  8  0 92  0  0
 0  0      0 30176016  22628 913916    0    0     0     0 1114 1553  8  0 92  0  0
 0  0      0 31735332  22636 913916    0    0     0     1 1059 1721  4  0 96  0  0
 0  0      0 31735292  22636 913916    0    0     0     0  954 1722  0  0 100  0  0
```


### PostgreSQL - Dataframe.to_sql(method=multi)

Writing to PostgreSQL using the pandas `to_sql(method=multi)` method.
```python
            result_df.to_sql(
                table_name,
                self._client.con,
                schema=schema_name,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
```

Command (where `pg_rh` is the name of a PostgreSQL connection configuration file):
```console
time data-validation -ll DEBUG validate row -sc=oravol -tc=pg \
-tbls=dvt_test.tab_vol_1m -pk=id --hash='*' \
--result-handler=pg_rh.pso_data_validator_results.results \
2>/tmp/debug.log

real	5m48.275s
user	4m30.577s
sys	0m2.820s
```

DVT stages:
```
04/03/2025 11:54:13 AM-DEBUG: Build config elapsed: 0.17s
04/03/2025 11:54:18 AM-DEBUG: Target query elapsed: 4.61s
04/03/2025 11:54:21 AM-DEBUG: Source query elapsed: 7.63s
04/03/2025 11:54:37 AM-DEBUG: Generate report elapsed: 15.79s
04/03/2025 11:59:59 AM-DEBUG: Write results to PostgreSQL elapsed: 322.34s
```

### PostgreSQL - COPY

Writing to PostgreSQL using the pandas `to_sql(method=COPY callable)` method using the method described here: https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method


Command (where `pg_rh` is the name of a PostgreSQL connection configuration file):
```console
time data-validation -ll DEBUG validate row -sc=oravol -tc=pg \
-tbls=dvt_test.tab_vol_1m -pk=id --hash='*' \
--result-handler=pg_rh.pso_data_validator_results.results \
2>/tmp/debug.log

real	0m50.537s
user	0m34.947s
sys	0m2.750s
```

DVT stages:
```
04/03/2025 02:35:58 PM-DEBUG: Build config elapsed: 0.19s
04/03/2025 02:36:03 PM-DEBUG: Target query elapsed: 4.66s
04/03/2025 02:36:06 PM-DEBUG: Source query elapsed: 7.67s
04/03/2025 02:36:22 PM-DEBUG: Generate report elapsed: 15.96s
04/03/2025 02:36:47 PM-DEBUG: Write results to PostgreSQL elapsed: 24.28s
```

`vmstat` output:
```
procs -----------memory---------- ---swap-- -----io---- -system-- ------cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 31628228  32032 1011728    0    0     7    15  295  445  5  1 95  0  0
 0  0      0 30959452  32048 1011720    0    0     0    11 4994 5501 13  3 83  0  0
 2  0      0 30394368  32048 1010832    0    0     0     1 1918 2496 23  5 72  0  0
 1  0      0 30135752  32068 1010836    0    0     0  7579 1381 1811 25  3 72  0  0
 1  0      0 30144104  32076 1010836    0    0     0    18 1740 1789 14  0 86  0  0
 0  0      0 30145512  32076 1010836    0    0     0     0 1661 1839 12  0 88  0  0
 0  0      0 31626232  32084 1010836    0    0     0     2 1162 1781  5  0 95  0  0
 0  0      0 31626232  32084 1010836    0    0     0     0  952 1723  0  0 100  0  0
```
