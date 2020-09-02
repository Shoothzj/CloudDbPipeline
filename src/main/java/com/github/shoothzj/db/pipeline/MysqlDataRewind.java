package com.github.shoothzj.db.pipeline;

import com.github.shoothzj.db.pipeline.api.AbstractDataRewind;
import com.github.shoothzj.db.pipeline.module.MysqlInfoDto;
import com.github.shoothzj.db.pipeline.module.TransformDto;
import com.github.shoothzj.db.pipeline.util.MysqlUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlDataRewind extends AbstractDataRewind {

    private final MysqlInfoDto mysqlInfoDto;

    private final TransformDto transformDto;

    private final HikariDataSource dataSource;

    public MysqlDataRewind(MysqlInfoDto mysqlInfoDto, TransformDto transformDto) {
        this.mysqlInfoDto = mysqlInfoDto;
        this.transformDto = transformDto;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(mysqlInfoDto.getJdbcUrl());
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        dataSource = new HikariDataSource(config);
    }

    @Override
    protected long processRewind() {
        final String tableName = mysqlInfoDto.getTableName();
        final String primaryKey = mysqlInfoDto.getPrimaryKey();
        long skip = -1;
        long count = 0;
        while (true) {
            try (Connection connection = dataSource.getConnection()) {
                if (skip == -1) {
                    // first
                    PreparedStatement prepareStatement = connection.prepareStatement(firstQuerySql(tableName, primaryKey, 50));
                    final ResultSet resultSet = prepareStatement.executeQuery();
                    int size = 0;
                    while (resultSet.next()) {
                        MysqlUtil.rewindResultSet(tableName, primaryKey, dataSource, resultSet, transformDto);
                        size++;
                    }
                    if (size == 0) {
                        break;
                    }
                    skip = size;
                    count += size;
                    log.info("size is [{}]", size);
                } else {
                    // two and there
                    PreparedStatement prepareStatement = connection.prepareStatement(subQuerySql(tableName, primaryKey, skip, 50));
                    final ResultSet resultSet = prepareStatement.executeQuery();
                    int size = 0;
                    while (resultSet.next()) {
                        MysqlUtil.rewindResultSet(tableName, primaryKey, dataSource, resultSet, transformDto);
                        size++;
                    }
                    if (size == 0) {
                        break;
                    }
                    skip += size;
                    count += size;
                    log.info("size is [{}]", size);
                }
            } catch (Exception e) {
                log.error("exec exception ", e);
            }
        }
        return count;
    }

    public String firstQuerySql(String tableName, String primaryKey, long pageSize) {
        // 子查询分页 SELECT * FROM $TABLE_NAME
        // WHERE $PRIMARY_KEY >= (SELECT $PRIMARY_KEY FROM $TABLE_NAME ORDER BY $PRIMARY_KEY asc LIMIT ${(page-1)*pagesize} )
        // ORDER BY $PRIMARY_KEY asc LIMIT $PAGE_SIZE
        String phase1Sql = "SELECT * FROM " + tableName;
        String phase2Sql = " WHERE " + primaryKey + " >= (SELECT " + primaryKey + " FROM " + tableName;
        String phase3Sql = " ORDER BY " + primaryKey + " asc LIMIT 1 )";
        String phase4Sql = " ORDER BY " + primaryKey + " asc LIMIT " + pageSize;
        return phase1Sql + phase2Sql + phase3Sql + phase4Sql + ";";
    }

    public String subQuerySql(String tableName, String primaryKey, long skip, long pageSize) {
        // 子查询分页 SELECT * FROM $TABLE_NAME
        // WHERE $PRIMARY_KEY >= (SELECT $PRIMARY_KEY FROM $TABLE_NAME ORDER BY $PRIMARY_KEY asc LIMIT ${(page-1)*pagesize} )
        // ORDER BY $PRIMARY_KEY asc LIMIT $PAGE_SIZE
        String phase1Sql = "SELECT * FROM " + tableName;
        String phase2Sql = " WHERE " + primaryKey + " >= (SELECT " + primaryKey + " FROM " + tableName;
        String phase3Sql = " ORDER BY " + primaryKey + " asc LIMIT " + skip + ",1 )";
        String phase4Sql = " ORDER BY " + primaryKey + " asc LIMIT " + pageSize;
        return phase1Sql + phase2Sql + phase3Sql + phase4Sql + ";";
    }


    @Override
    protected void close() {
        dataSource.close();
    }
}
