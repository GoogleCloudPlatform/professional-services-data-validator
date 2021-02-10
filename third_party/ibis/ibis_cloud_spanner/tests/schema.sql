CREATE TABLE students_pointer 
  ( 
     id             INT64, 
     name           STRING(30), 
     division       INT64, 
     marks           INT64, 
     exam           STRING(30), 
     overall_pointer FLOAT64, 
     date_of_exam    TIMESTAMP 
  )PRIMARY KEY(id); 

INSERT into students_pointer(id,name,division,marks,exam,overall_pointer,date_of_exam) values(101,'Ross',12,500,'Biology',9.8,'2002-02-10 15:30:00+00');

INSERT into students_pointer(id,name,division,marks,exam,overall_pointer,date_of_exam) values(102,'Rachel',14,460,'Chemistry',9.9,'2018-04-22');

INSERT into students_pointer(id,name,division,marks,exam,overall_pointer,date_of_exam) values(103,'Chandler',12,480,'Biology',8.2,'2016-04-14');

INSERT into students_pointer(id,name,division,marks,exam,overall_pointer,date_of_exam) values(104,'Monica',12,390,'Maths',9.2,'2019-04-29');

INSERT into students_pointer(id,name,division,marks,exam,overall_pointer,date_of_exam) values(105,'Joey',16,410,'Maths',9.7,'2019-06-21');

INSERT into students_pointer(id,name,division,marks,exam,overall_pointer,date_of_exam) values(106,'Phoebe',10,490,'Chemistry',9.6,'2019-02-09');






CREATE TABLE awards 
  ( 
     id         INT64, 
     award_name STRING(20) 
  )PRIMARY KEY(id); 

Insert into awards (id,award_name) values (101,'LOTUS')
Insert into awards (id,award_name) values (102,'ROSE')






CREATE TABLE functional_alltypes (
	id INT64,
	bigint_col INT64,
	bool_col BOOL,
    date DATE,
	date_string_col STRING(MAX),
	double_col NUMERIC,
	float_col NUMERIC,
	index INT64,
	int_col INT64,
	month INT64,
	smallint_col INT64,
	string_col STRING(MAX),
	timestamp_col TIMESTAMP,
	tinyint_col INT64,
	Unnamed0 INT64,
	year INT64
) PRIMARY KEY (id)

INSERT into functional_alltypes (id ,bigint_col ,bool_col ,date ,date_string_col ,double_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year ) values
(1,10001,TRUE,'2016-02-09','01/01/2001',2.5,12.16,101,21,4,16,'David','2002-02-10 15:30:00+00',6,99,2010)

INSERT into functional_alltypes (id ,bigint_col ,bool_col ,date ,date_string_col ,double_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year ) values
(2,10002,FALSE,'2016-10-10','02/02/2002',2.6,13.16,102,22,5,18,'Ryan','2009-02-12 10:06:00+00',7,98,2012)

INSERT into functional_alltypes (id ,bigint_col ,bool_col ,date ,date_string_col ,double_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year ) values
(3,10003,TRUE,'2018-02-09','03/03/2003',9.5,44.16,201,41,6,56,'Steve','2010-06-10 12:12:00+00',12,66,2006)

INSERT into functional_alltypes (id ,bigint_col ,bool_col ,date ,date_string_col ,double_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year ) values
(4,10004,TRUE,'2018-10-10','04/04/2004',9.6,45.16,202,42,9,58,'Chandler','2014-06-12 10:04:00+00',14,69,2009)


INSERT into functional_alltypes (id ,bigint_col ,bool_col ,date ,date_string_col ,double_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year ) values
(5,10005,FALSE,'2020-06-12','05/05/2005',6.6,66.12,401,62,12,98,'Rose','2018-02-10 10:06:00+00',16,96,2012)

INSERT into functional_alltypes (id ,bigint_col ,bool_col ,date ,date_string_col ,double_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year ) values
(6,10006,TRUE,'2020-12-12','06/06/2006',6.9,66.19,402,69,14,99,'Rachel','2019-04-12 12:09:00+00',18,99,2014)






CREATE TABLE array_table (
    string_col ARRAY<STRING(MAX)>,
    int_col ARRAY<INT64>,
    id INT64,
) PRIMARY KEY (id)

INSERT into array_table (id,string_col,int_col) values (1,['Peter','David'],[11,12])
INSERT into array_table_1 (id,string_col,int_col) values (2,['Raj','Dev','Neil'],[1,2,3])
