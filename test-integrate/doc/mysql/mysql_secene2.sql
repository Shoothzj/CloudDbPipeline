DROP TABLE IF EXISTS old_table_etl_scene_2;
CREATE TABLE old_table_etl_scene_2
(
    id        LONG,
    name    TEXT,
    createTime   LONG
);

DROP PROCEDURE IF EXISTS procedure_etl_scene_2;

CREATE PROCEDURE procedure_etl_scene_2()
BEGIN
    DECLARE i INT;
    SET i = 1;
    WHILE (i <= 1000)
        DO
            SET i = i + 1;
            INSERT INTO old_table_etl_scene_2 (id, name, createTime) VALUES (i, CONCAT('STR', i), unix_timestamp());
        end while;
end;

call procedure_etl_scene_2();