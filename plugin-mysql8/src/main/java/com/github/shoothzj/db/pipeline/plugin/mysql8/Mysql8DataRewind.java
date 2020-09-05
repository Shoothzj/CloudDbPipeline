package com.github.shoothzj.db.pipeline.plugin.mysql8;

import com.github.shoothzj.db.pipeline.core.AbstractRewind;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;
import com.github.shoothzj.db.pipeline.api.module.db.MysqlInfoDto;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import com.github.shoothzj.db.pipeline.plugin.mysql8.util.MysqlCalculateUtil;
import com.github.shoothzj.db.pipeline.plugin.mysql8.util.MysqlConcatUtil;
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
public class Mysql8DataRewind<PT> extends AbstractRewind<MysqlInfoDto, PT> {

    private MysqlInfoDto mysqlInfoDto;

    private TransformDto transformDto;

    private HikariDataSource dataSource;

    private String tableName;

    private String primaryKey;

    public Mysql8DataRewind() {
    }

    @Override
    public void init(MysqlInfoDto mysqlInfoDto, TransformDto transformDto) {
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
    public SourceBrief<PT> detectBrief() {
        return null;
    }

    @Override
    public boolean rewind() {
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
        return count > 0;
    }

    @Override
    public boolean rewind(PT start, PT end) {
        return false;
    }

    private void processSingleItem(ResultSet resultSet) {
        MysqlCalculateUtil.rewindResultSet(tableName, primaryKey, dataSource, resultSet, transformDto);
    }


    @Override
    public void close() {
        dataSource.close();
    }
}
