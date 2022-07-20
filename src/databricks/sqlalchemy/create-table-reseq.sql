/* alternate table/data for sample app  */

USE george_chow_dbtest;

CREATE TABLE sample_numtypes 
 (
	f_byte     BYTE,
	f_boolean  BOOLEAN,
	f_short    SHORT,
	f_int      INT,
	f_long     LONG,
	f_float    FLOAT,
	f_decimal  DECIMAL(10,2)
 );

INSERT INTO sample_numtypes VALUES
 ( 125, TRUE, 32700, 2001002003, 9001002003004005006, 1E30, 1.5 ),
 ( -125, FALSE, -32700, -2001002003, -9001002003004005006, 1E-30, -1.5 ),
 ( 125, TRUE, 32700, 2001002003, 9001002003004005006, -1E30, 1.5 );

SELECT * FROM sample_numtypes;

DESCRIBE sample_numtypes;

