CREATE DATABASE pso_data_validator;
DROP TABLE pso_data_validator.dvt_core_types;
CREATE TABLE pso_data_validator.dvt_core_types
(   id              int NOT NULL PRIMARY KEY
,   col_int8        tinyint
,   col_int16       smallint
,   col_int32       int
,   col_int64       bigint
,   col_dec_20      decimal(20)
,   col_dec_38      decimal(38)
,   col_dec_10_2    decimal(10,2)
,   col_float32     float
,   col_float64     double
,   col_varchar_30  varchar(30)
,   col_char_2      char(2)
,   col_string      varchar(21000)
,   col_date        date
,   col_datetime    datetime(3)
,   col_tstz        timestamp(3)
) COMMENT='Core data types integration test table';

SET time_zone = '+00:00';
INSERT INTO pso_data_validator.dvt_core_types VALUES
(1,1,1,1,1
 ,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
 ,'Hello DVT','A ','Hello DVT'
 ,'1970-01-01','1970-01-01 00:00:01','1970-01-01 01:00:01'),
(2,2,2,2,2
 ,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
 ,'Hello DVT','B ','Hello DVT'
 ,'1970-01-02','1970-01-02 00:00:02','1970-01-02 02:00:02'),
(3,3,3,3,3
 ,12345678901234567890,1234567890123456789012345,123.33,123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,'1970-01-03','1970-01-03 00:00:03','1970-01-03 03:00:03');
