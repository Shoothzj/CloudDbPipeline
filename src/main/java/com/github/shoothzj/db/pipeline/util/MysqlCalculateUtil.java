package com.github.shoothzj.db.pipeline.util;

import com.github.shoothzj.db.pipeline.module.MapDto;
import com.github.shoothzj.db.pipeline.module.TransformDto;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlCalculateUtil {

    // 更新数据
    public static void rewindResultSet(String tableName, String primaryKey, HikariDataSource dataSource, ResultSet resultSet, TransformDto transformDto) {
        // 获得map
        final List<MapDto> dtoMap = transformDto.getMap();
        // 根据PrimaryKey更新
        String updateSql = MysqlConcatUtil.getUpdateSql(tableName, dtoMap.stream().map(MapDto::getFieldName).collect(Collectors.toList()), primaryKey);
        log.debug("update sql is [{}]", updateSql);
        try (final Connection connection = dataSource.getConnection()) {
            final PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
            int i = 1;
            for (MapDto mapDto : dtoMap) {
                final String fieldName = mapDto.getFieldName();
                final Object returnObj = CalculateUtil.processMap(resultSet.getObject(fieldName), mapDto);
                preparedStatement.setObject(i, returnObj);
                i++;
            }
            preparedStatement.setObject(i, resultSet.getObject(primaryKey));
            final int executeUpdate = preparedStatement.executeUpdate();
            if (executeUpdate == 0) {
                log.error("no one updated");
            }
            log.debug("execute update is [{}]", executeUpdate);
        } catch (Exception e) {
            log.error("execute exception ,error is ", e);
        }
    }

}
