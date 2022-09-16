#
# **1.Installation**

Snowflake is an enterprise cloud based data-warehousing platform. Connection to snowflake is established using the snowflake python connector[1] which is available via PyPi, which is prerequisite for the connector use and should be installed using pip as follows:

    pip install snowflake-connector-python

The connector additionally uses the SQLAlchemy library provided by the snowflake team[2], which is available as an extension of SQLAlchemy available in PyPi and can be installed using pip as follows:

    pip install snowflake-sqlalchemy

For establishing connection with snowflake datastore, there are certain credentials required, listed as follows:

    USERNAME
    PASSWORD
    ACCOUNT
    DATABASE
    SCHEMA

The Snowflake client is accessible through the ibis.ibis_snowflake namespace. The ibis.sql.ibis_snowflake.connect can connect to the database with a SQLAlchemy[2] compatible connection string to create a client connection which is in format:

    snowflake://{user}:{password}@{account}/{database}

#
# **2.Code snippet for connecting to Snowflake using ibis:-**

    import ibis
    import os
    from ibis.sql.ibis_snowflake.api import connect
    db=connect("username", "account", "password", "database", "schema")
    tb_name=db.table("students")
    result=tb_name.count().execute()
    print(result)**

# Snowflake DVT connection
```
{
    # Configuration Required for All Data Sources
    "source_type": "Snowflake",

    # Connection Details
    "user": "my-user",
    "password": "my-password",
    "account": "Snowflake account to connect to"
    "database":"my-db"
    "schema": "my-schema"
}
```

# **3.Usage**

- Schema for the &#39;students_pointer&#39; table:-

        CREATE TABLE students_pointer
        (
        id INTEGER,
        name VARCHAR(30),
        division INTEGER,
        marks INTEGER,
        exam VARCHAR(30),
        overall\_pointer FLOAT,
        date\_of\_exam TIMESTAMP
        );

- Schema for the &#39;awards&#39; table:-

        CREATE TABLE awards
        (
        id INTEGER,
        award_name VARCHAR(20)
        );

- Queries showing implementation of different functions

        import ibis
        from ibis.sql.ibis_snowflake.api import connect
        con=connect("username", "account", "password", "database", "schema")
        table_details=con.table('students_pointer')
        table_records=con.table('students_pointer').execute()

# 3.1 Print the first two records of the table "students_pointer"

    result_q1=table_details.limit(2).execute()
    print("Initial Two Records:\n" , result_q1)

# 3.2 Print the highest scorers in each of the exams(biology,chemistry and maths)

    result_q2=table_details.group_by("exam").marks.max().execute()
    print( Highest scorers in each of the exams:\n , result_q2)

# 3.3 Print the students in division &quot;12&quot; who have scored more than 490 marks

    cond1=table_details.division.isin([12])
    cond2=table_details.marks > 500
    result_q3=table_details.filter([cond1,cond2]).execute()
    print("Students(division=12 & marks>490 ):\n " , result_q3)

# 3.4 Print the student records where division is sorted ascending manner and overall\_pointer in descending manner

    from ibis import desc
    result_q4=table_details.sort_by(['division',desc(table_details.overall_pointer)]).execute()
    print('Result:\n '' , result_q4)

# 3.5 Selecting particular columns from the table

    result_q5=table_details.select(['name','marks']).execute()
    print("Result:\n" , result_q5)

# 3.6 Perform the &quot;Join operation&quot; on the tables

    tb1=con.table('students_pointer')
    tb2=con.table('awards')
    join_expr = tb1.id == tb2.id
    joined = tb1.inner_join(tb2, join_expr)
    table_ref = joined[tb1, tb2.award_name.name('award_name')]
    result_q6=table_ref.select(['id','name','award_name']).execute()
    print("Print the students' records who have won awards:\n " , result_q6)

# 3.7 Write a query to find :-

i. Number of the people given the exam

    t = con.table('students_pointer')
    d = table_details.overall_pointer
    expr = (t.group_by('exam')
    .aggregate([d.min(), d.max(), d.mean(), t.count()])
    .sort_by('exam'))
    result_q7=expr.execute()
    print("Result:\n " , result_q7)

# 3.8 Extract the information of students whose name starts with &#39;R&#39;

    result_q8= table_details[table_details.name.like("R%")].execute()
    print("Result:\n " , result_q8)

# 3.9 Convert the values in &quot;exam&quot; Column into uppercase

    result_q9=table_details.select(['id','name',table_details.exam.upper().name("uppercase_letter")]).execute()
    print("Result:\n " , result_q9)

# 3.10 Print the &quot;year of the examination&quot; for each of the students

    result_q10=table_details[table_details.id,table_details.name,
    table_details.date_of_exam,table_details.date_of_exam.year()
    .name('year_of_exam')].execute()
    print("Result:\n" , result_q10)

# 3.11 Print the number of unique names of exam

    t = con.table("students_pointer")
    result_q11=t.exam.nunique().execute()
    print("Result:\n " , result_q11)

# 3.12 Print the distinct exam names

    t = con.table("students_pointer")
    result_q12=t.exam.distinct().execute()
    print("Result:\n " , result_q12)

# 3.13 Print the name of students in the list/group format

    t = con.table("students_pointer")
    result_q13=t.name.group_concat().execute()
    print("Result:\n " , result_q13)

# 3.14 Convert the values of timestamp column in the given format

    t=con.table('students_pointer')
    result_q14=t[t.date_of_exam.strftime('%Y%m%d %H').name('New_col')].execute()
    print("Result:\n  ", result_q14)

# 3.15 Extract the name and number of the day of the week from timestamp column

    t=con.table('students_pointer')
    result_q15=t[t.name,t.date_of_exam.day_of_week.index().name('weekday_number'),
    t.date_of_exam.day_of_week.full_name().name('weekday_name')].execute()
    print("Result:\n" , result_q15)

#
# **4.Steps to run Test scripts**

Schemas of the tables created in the database: Create table named "functional_alltypes" in the database with the below schema:-

    CREATE TABLE FUNCTIONAL_ALLTYPES
    (
    'index' NUMBER(19),
    'unnamed: 0' NUMBER(19),
    id NUMBER(10),
    bool_col VARCHAR(4),
    tinyint_col NUMBER(5),
    smallint_col NUMBER(5),
    int_col NUMBER(10),
    bigint_col NUMBER(19),
    float_col FLOAT(23),
    double_col DOUBLE PRECISION,
    date_string_col VARCHAR(10),
    string_col VARCHAR(10),
    timestamp_col TIMESTAMP,
    year INTEGER,
    month INTEGER
    );

- Write the connection parameters(database credentials) in the following two files:-

snowflake/tests/conftest.py

    IBIS_SNOWFLAKE_USER = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_USER', os.environ.get('SF_USER', 'snowflake')
    )
    IBIS_SNOWFLAKE_PASS = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_PASSWORD', os.environ.get('SF_PASWORD', 'snowflake')
    )
    IBIS_SNOWFLAKE_ACCOUNT = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_ACCOUNT', os.environ.get('SF_ACCOUNT', 'myAccount')
    )
    IBIS_SNOWFLAKE_SCHEMA = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_SCHEMA', os.environ.get('SF_SCHEMA', 'PUBLIC')
    )
    IBIS_SNOWFLAKE_DATABASE = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_DATABASE', os.environ.get('SF_DB', 'TEST')
    )
    
    @pytest.fixture(scope='session')
    def con():
    ibis.sql.ibis_oracle.api.connect("user_name","password","database_name")

oracle/tests/udf/test_client.py

    IBIS_SNOWFLAKE_USER = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_USER', os.environ.get('SF_USER', 'snowflake')
    )

    IBIS_SNOWFLAKE_PASS = os.environ.get(
    IBIS_TEST_SNOWFLAKE_PASSWORD', os.environ.get('SF_PASWORD', 'snowflake')
    )

    IBIS_SNOWFLAKE_ACCOUNT = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_ACCOUNT', os.environ.get('SF_ACCOUNT', 'myAccount')
    )

    IBIS_SNOWFLAKE_SCHEMA = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_SCHEMA', os.environ.get('SF_SCHEMA', 'PUBLIC')
    )

    IBIS_SNOWFLAKE_DATABASE = os.environ.get(
    'IBIS_TEST_SNOWFLAKE_DATABASE', os.environ.get('SF\_DB', 'TEST')
    )

- To run individual test file:-

        pytest test_client.py

- To run whole the test folder:-

        pytest tests

#
# **5.Limitations**

Snowflake does not support extracting milliseconds from timestamps[3].

When extracting day names from the timestamp, snowflake returns only three-lettered day names[3].

For extracting text using regex, the position of the group to be returned must be a positive number i.e the position index starts from 1

#
# **6. References**

[1] [Snowflake Python Connector](https://docs.snowflake.com/en/user-guide/python-connector.html)

[2] [Snowflake SQLAlchemy](https://docs.snowflake.com/en/user-guide/sqlalchemy.html)

[3] [Snowflake time formats](https://docs.snowflake.com/en/sql-reference/functions-date-time.html#supported-date-and-time-parts)
