package com.github.shoothzj.db.pipeline.util;

import com.github.shoothzj.db.pipeline.module.MapDto;
import com.github.shoothzj.db.pipeline.module.TransformDto;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlUtil {

    // 更新数据
    public static void rewindResultSet(String tableName, String primaryKey, HikariDataSource dataSource, ResultSet resultSet, TransformDto transformDto) {
        // 获得map
        final List<MapDto> dtoMap = transformDto.getMap();
        // 根据PrimaryKey更新
        // UPDATE $TABLE_NAME SET $field = ? WHERE $PRIMARY_KEY = ?;
        String updateSql = getUpdateSql(tableName, dtoMap.stream().map(MapDto::getFieldName).collect(Collectors.toList()), primaryKey);
        log.debug("update sql is [{}]", updateSql);
        try (final Connection connection = dataSource.getConnection()) {
            final PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
            int i = 1;
            for (MapDto mapDto : dtoMap) {
                final String transformType = mapDto.getTransformType();
                final String value = mapDto.getValue();
                if (transformType.equals("function")) {
                    if (value.equals("UUID")) {
                        preparedStatement.setObject(i, UUID.randomUUID().toString());
                    } else {
                        throw new IllegalArgumentException("not implement yet.");
                    }
                } else if (transformType.equals("constant")) {
                    preparedStatement.setObject(i, 10);
                } else {
                    throw new IllegalArgumentException("not implement yet.");
                }
                i++;
            }
            preparedStatement.setObject(i, resultSet.getObject(primaryKey));
            final int executeUpdate = preparedStatement.executeUpdate();
            if (executeUpdate == 0) {
                log.error("no one updated");
            }
            log.info("execute update is [{}]", executeUpdate);
        } catch (Exception e) {
            log.error("execute exception ,error is ", e);
        }
    }

    public static String getUpdateSql(String tableName, List<String> field, String primaryKey) {
        String phase1Sql = "UPDATE " + tableName + " SET ";
        String phase2Sql = field.stream().map(str -> str + " = ?").collect(Collectors.joining(","));
        String phase3Sql = " WHERE " + primaryKey + " = ?";
        return phase1Sql + phase2Sql + phase3Sql;
    }

}
