import ibis.expr.datatypes as dt

SCHEMA_SQL = """
SELECT TRIM(c.colname) AS colname, 
CASE 
  WHEN MOD(coltype,256)=0 THEN "CHAR" 
  WHEN MOD(coltype,256)=1 THEN "SMALLINT" 
  WHEN MOD(coltype,256)=2 THEN "INTEGER" 
  WHEN MOD(coltype,256)=3 THEN "FLOAT" 
  WHEN MOD(coltype,256)=4 THEN "SMALLFLOAT" 
  WHEN MOD(coltype,256)=5 THEN "DECIMAL" 
  WHEN MOD(coltype,256)=6 THEN "SERIAL" 
  WHEN MOD(coltype,256)=7 THEN "DATE" 
  WHEN MOD(coltype,256)=8 THEN "MONEY" 
  WHEN MOD(coltype,256)=9 THEN "NULL" 
  WHEN MOD(coltype,256)=10 THEN "DATETIME" 
  WHEN MOD(coltype,256)=11 THEN "BYTE" 
  WHEN MOD(coltype,256)=12 THEN "TEXT" 
  WHEN MOD(coltype,256)=13 THEN "VARCHAR" 
  WHEN MOD(coltype,256)=14 THEN "INTERVAL" 
  WHEN MOD(coltype,256)=15 THEN "NCHAR" 
  WHEN MOD(coltype,256)=16 THEN "NVARCHAR" 
  WHEN MOD(coltype,256)=17 THEN "INT8" 
  WHEN MOD(coltype,256)=18 THEN "SERIAL8" 
  WHEN MOD(coltype,256)=19 THEN "SET" 
  WHEN MOD(coltype,256)=20 THEN "MULTISET" 
  WHEN MOD(coltype,256)=21 THEN "LIST" 
  WHEN MOD(coltype,256)=22 THEN "ROW (unnamed)" 
  WHEN MOD(coltype,256)=23 THEN "COLLECTION" 
  WHEN MOD(coltype,256)=40 THEN "LVARCHAR fixed-length opaque types" 
  WHEN MOD(coltype,256)=41 THEN "BLOB, BOOLEAN, CLOB variable-length opaque types" 
  WHEN MOD(coltype,256)=43 THEN "LVARCHAR (client-side only)" 
  WHEN MOD(coltype,256)=45 THEN "BOOLEAN" 
  WHEN MOD(coltype,256)=52 THEN "BIGINT" 
  WHEN MOD(coltype,256)=53 THEN "BIGSERIAL" 
  WHEN MOD(coltype,256)=2061 THEN "IDSSECURITYLABEL"
  WHEN MOD(coltype,256)=4118 THEN "ROW (named)" 
  ELSE TO_CHAR(coltype)
END AS datatype
  FROM informix.systables  AS t
  JOIN informix.syscolumns AS c ON t.tabid = c.tabid
 WHERE t.tabtype = "T" AND t.tabid >= 100 and t.tabname="{}"
 ORDER BY t.tabname, c.colno
"""


def to_ibis_type(ifx_type):

  if ifx_type is None or ifx_type.lower()=='null':
    return dt.null

  ifx_type = str(ifx_type).lower().strip()

  if ifx_type in ['char','byte','text','varchar','interval','nvarchar','nchar']:
    return dt.string
  elif ifx_type in ['smallint','int8','serial8']:
    return dt.int8
  elif ifx_type=='integer':
    return dt.int32
  elif ifx_type=='float':
    return dt.float64
  elif ifx_type=='smallfloat':
    return dt.float16
  elif ifx_type in ['decimal','money']:
    return dt.Decimal(20, 4)
  elif ifx_type=='serial':
    return dt.int32
  elif ifx_type=='date':
    return dt.date
  elif ifx_type=='float':
    return dt.float64
  elif ifx_type=='datetime':
    return dt.timestamp
  elif ifx_type=='int8':
    return dt.int8
  elif ifx_type in ['set','multiset']:
    return dt.Set()
  elif ifx_type=='list':
    return dt.List()
  elif ifx_type in 'lvarchar':
    return dt.string
  elif ifx_type=='boolean':
    return dt.boolean
  elif ifx_type in ['bigint','bigserial']:
    return dt.int64
  else:
    return dt.string
