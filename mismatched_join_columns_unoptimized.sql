WITH t10530 AS (
    SELECT id, second_join_value 
    FROM primary_table
), 
t33337 AS (
    SELECT id, primary_table_fk, some_value FROM second_table
), 
t41053 AS (
    SELECT id, second_join_value_fk, other_value 
    FROM third_table
) 
SELECT COALESCE(t10530.id, NULL) AS the_primary_id, 'the_primary_id', 
COALESCE(t33337.some_value, NULL) AS second_table_value, 'second_table_value', 
COALESCE(t10530.id, NULL) AS the_primary_id, 'the_primary_id' 
FROM t10530 
LEFT JOIN t33337 ON t10530.id = t33337.primary_table_fk 
LEFT JOIN t41053 ON t33337.second_join_value = t41053.second_join_value_fk 
WHERE COALESCE(t33337.some_value, NULL) LIKE '%abc%' AND 
COALESCE(t33337.some_value, NULL) <> X AND 
NOT COALESCE(t33337.some_value, NULL) IS NULL;

WITH t10530 AS (
    SELECT id, second_join_value 
    FROM primary_table
), 
t33337 AS (
    SELECT id, primary_table_fk, some_value 
    FROM second_table
), 
t41053 AS (
    SELECT id, second_join_value_fk, other_value 
    FROM third_table
) 
SELECT t10530.id AS the_primary_id, 
t33337.some_value AS second_table_value, 
t10530.id AS the_primary_id 
FROM t10530 
LEFT JOIN t33337 ON t10530.id = t33337.primary_table_fk 
LEFT JOIN t41053 ON t33337.second_join_value = t41053.second_join_value_fk 
WHERE COALESCE(t33337.some_value, NULL) LIKE '%abc%' 
AND COALESCE(t33337.some_value, NULL) <> X 
AND NOT COALESCE(t33337.some_value, NULL) IS NULL;
