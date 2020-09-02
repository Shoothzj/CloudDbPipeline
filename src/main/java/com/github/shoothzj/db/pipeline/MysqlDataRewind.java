package com.github.shoothzj.db.pipeline;

import com.github.shoothzj.db.pipeline.api.AbstractDataRewind;
import com.github.shoothzj.db.pipeline.module.MysqlInfoDto;
import com.github.shoothzj.db.pipeline.module.TransformDto;
import com.github.shoothzj.db.pipeline.util.MysqlCalculateUtil;
import com.github.shoothzj.db.pipeline.util.MysqlConcatUtil;
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
public class MysqlDataRewind extends AbstractDataRewind<ResultSet> {

    private final MysqlInfoDto mysqlInfoDto;

    private final TransformDto transformDto;

    private final HikariDataSource dataSource;

    private final String tableName;

    private final String primaryKey;

    public MysqlDataRewind(MysqlInfoDto mysqlInfoDto, TransformDto transformDto) {
        this.mysqlInfoDto = mysqlInfoDto;
        this.transformDto = transformDto;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(mysqlInfoDto.getJdbcUrl());
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        dataSource = new HikariDataSource(config);
        tableName = mysqlInfoDto.getTableName();
        primaryKey = mysqlInfoDto.getPrimaryKey();
    }

    @Override
    protected long processRewind() {
        long skip = 0;
        long count = 0;
        while (true) {
            try (Connection connection = dataSource.getConnection()) {
                final PreparedStatement prepareStatement;
                if (skip == 0) {
                    // first
                    prepareStatement = connection.prepareStatement(MysqlConcatUtil.firstQuerySql(tableName, primaryKey, 50));
                } else {
                    // two and there
                    prepareStatement = connection.prepareStatement(MysqlConcatUtil.subQuerySql(tableName, primaryKey, skip, 50));
                }
                final ResultSet resultSet = prepareStatement.executeQuery();
                int size = 0;
                while (resultSet.next()) {
                    processSingleItem(resultSet);
                    size++;
                }
                if (size == 0) {
                    break;
                }
                skip += size;
                count += size;
                log.info("size is [{}]", size);
            } catch (Exception e) {
                log.error("exec exception ", e);
            }
        }
        return count;
    }

    @Override
    protected void processSingleItem(ResultSet resultSet) {
        MysqlCalculateUtil.rewindResultSet(tableName, primaryKey, dataSource, resultSet, transformDto);
    }


    @Override
    protected void close() {
        dataSource.close();
    }
}
