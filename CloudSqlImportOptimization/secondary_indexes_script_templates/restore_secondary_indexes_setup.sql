/* Copyright 2019, Roger Padilla Camacho - rogerjose81@gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. */

-- Script that generates the commands needed to restore all secondary indexes in
-- all tables of a Database.
SET sql_mode = '';

SELECT
  CONCAT(
    'ALTER TABLE ',
    '`', S.TABLE_NAME, '`',
    ' ADD ',
    IF(
      NON_UNIQUE = 1,
      CASE UPPER(INDEX_TYPE)
        WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
        WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
        ELSE CONCAT('INDEX ', '`',INDEX_NAME, '`',' USING ', INDEX_TYPE)
      END,
      CONCAT('UNIQUE INDEX ', '`',INDEX_NAME, '`',' USING ', INDEX_TYPE)
    ),
    '(',
    GROUP_CONCAT(
      DISTINCT
        CONCAT(
          '',
          IF(
            SUB_PART > 0,
            CONCAT('`', S.COLUMN_NAME, '`','(', SUB_PART, ')'),
            S.COLUMN_NAME
          ),
          ''
        )
      ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '
    ),
    ');'
  ) AS 'Show_Add_Indexes'
FROM information_schema.STATISTICS AS S
INNER JOIN information_schema.COLUMNS AS C ON C.TABLE_NAME = S.TABLE_NAME
  AND S.COLUMN_NAME = C.COLUMN_NAME
  AND S.TABLE_SCHEMA = C.TABLE_SCHEMA
WHERE S.TABLE_SCHEMA = '<source_database_name>'
  AND S.TABLE_NAME = '<source_database_table>'
  AND INDEX_NAME != 'PRIMARY'
  AND COLUMN_KEY != 'PRI'
GROUP BY S.TABLE_NAME, INDEX_NAME
ORDER BY S.TABLE_NAME ASC, INDEX_NAME ASC;
