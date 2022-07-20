/* table/data for sample app */

USE george_chow_dbtest;

CREATE TABLE sample_numtypes 
 (
	f_byte     BOOLEAN,
	f_short    BOOLEAN,
	f_int      BOOLEAN,
	f_long     BOOLEAN,
	f_float    BOOLEAN,
	f_decimal  DECIMAL(10,2),
	f_boolean  INT
 );

INSERT INTO sample_numtypes VALUES
 ( 125, 32700, 2001002003, 9001002003004005006, 1E30, 1.5, TRUE ),
 ( -125, -32700, -2001002003, -9001002003004005006, 1E-30, -1.5, FALSE ),
 ( 125, 32700, 2001002003, 9001002003004005006, -1E30, 1.5, TRUE );

SELECT * FROM sample_numtypes;

DESCRIBE sample_numtypes;

