/* table/data for sample app */

USE george_chow_dbtest;

DROP TABLE IF EXISTS sample_numtypes; 

CREATE TABLE IF NOT EXISTS sample_numtypes 
 (
	f_byte     BYTE,
	f_short    SHORT,
	f_int      INT,
	f_long     LONG,
	f_float    FLOAT,
	f_double    DOUBLE,
	f_decimal  DECIMAL(10,2),
	f_boolean  BOOLEAN
 );

INSERT INTO sample_numtypes VALUES
 ( 125, 32700, 2001002003, 9001002003004005006, 1E30, 1E308, 1.5, TRUE ),
 ( -125, -32700, -2001002003, -9001002003004005006, 1E-30, 1E-308, -1.5, FALSE ),
 ( 125, 32700, 2001002003, 9001002003004005006, -1E30, -1E308, 1.5, TRUE );

SELECT * FROM sample_numtypes;

DESCRIBE sample_numtypes;


DROP TABLE IF EXISTS sample_strtypes; 

CREATE TABLE sample_strtypes
 (
	f_string     STRING,
	f_date       DATE,
	f_timestamp  TIMESTAMP
 );
	-- f_interval   INTERVAL DAY TO SECOND

INSERT INTO sample_strtypes VALUES
 ( 'Everest', '1953-05-29', '1953-05-29T11:30' ),
 ( 'Mariana Trench', '1960-01-23', '1960-01-23T13:06' ),
 ( 'Moon landing', '1969-07-20', '1969-07-20T20:17' );

--  ( 'Everest', '1953-05-29', '1953-05-29T11:30', '3 0:0:0' ),
--  ( 'Mariana Trench', '1960-01-23', '1960-01-23T13:06', '0 5:00:00' ),
--  ( 'Moon landing', '1969-07-20', '1969-07-20T20:17', '0 21:36:0' );

SELECT * FROM sample_strtypes;

DESCRIBE sample_strtypes;
