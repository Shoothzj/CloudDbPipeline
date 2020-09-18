CREATE TABLE old_table_etl_scene_1
(
    id        LONG,
    oldInt    INT,
    oldLong   LONG,
    oldString VARCHAR(200)
);

INSERT INTO old_table_etl_scene_1 (id, oldInt, oldLong, oldString)
VALUES (1, 1, 1, 'st1');

DROP PROCEDURE IF EXISTS procedure_etl_scene_1;

CREATE PROCEDURE procedure_etl_scene_1()
BEGIN
    DECLARE i INT;
    SET i = 1;
    WHILE (i <= 10000)
        DO
            SET i = i + 1;
            INSERT INTO old_table_etl_scene_1 (id, oldInt, oldLong, oldString) VALUES (i, i, i, CONCAT('STR', i));
        end while;
end;

call procedure_etl_scene_1();