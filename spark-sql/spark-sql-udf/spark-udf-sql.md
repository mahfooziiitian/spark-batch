# Udf

## SQL UDFs as constants

    CREATE FUNCTION blue()
    RETURNS STRING
    COMMENT 'Blue color code'
    LANGUAGE SQL
    RETURN '0000FF';

    SELECT blue();

## SQL UDF encapsulating expressions

    CREATE FUNCTION to_hex(x INT COMMENT 'Any number between 0 - 255')
        RETURNS STRING
        COMMENT 'Converts a decimal to a hexadecimal'
        CONTAINS SQL DETERMINISTIC
        RETURN lpad(hex(least(greatest(0, x), 255)), 2, 0)

## example2

    CREATE FUNCTION rgb_to_hex(r INT, g INT, b INT)
    RETURNS STRING
    COMMENT 'Converts an RGB color to a hex color code'
    RETURN CONCAT(to_hex(r), to_hex(g), to_hex(b))

## SQL UDF reading from tables

    CREATE FUNCTION from_rgb(rgb STRING
                                COMMENT 'an RGB hex color code')
    RETURNS STRING
    COMMENT 'Translates an RGB color code into a color name'
    RETURN DECODE(rgb, 'FF00FF', 'magenta',
                        'FF0080', 'rose');

CREATE TABLE colors(rgb STRING NOT NULL, name STRING NOT NULL);
INSERT INTO colors VALUES
  ('FF00FF', 'magenta'),
  ('FF0080', 'rose'),
  ('BFFF00', 'lime'),
  ('7DF9FF', 'electric blue');

CREATE OR REPLACE FUNCTION
from_rgb(rgb STRING COMMENT 'an RGB hex color code')
   RETURNS STRING
   READS SQL DATA SQL SECURITY DEFINER
   COMMENT 'Translates an RGB color code into a color name'
   RETURN SELECT FIRST(name) FROM colors WHERE rgb = from_rgb.rgb;

SELECT from_rgb(rgb)
  FROM VALUES('7DF9FF'),
  ('BFFF00') AS codes(rgb);

## SQL Table UDF

INSERT INTO colors VALUES ('BFFF00', 'citron vert');

CREATE OR REPLACE FUNCTION
     from_rgb(rgb STRING COMMENT 'an RGB hex color code')
   RETURNS TABLE(name STRING COMMENT 'color name')
   READS SQL DATA SQL SECURITY DEFINER
   COMMENT 'Translates an RGB color code into a color name'
   RETURN SELECT name FROM colors WHERE rgb = from_rgb.rgb;

##

SELECT rgb, from_rgb.name
    FROM VALUES('7DF9FF'),
               ('BFFF00') AS codes(rgb),
         LATERAL from_rgb(codes.rgb);  
