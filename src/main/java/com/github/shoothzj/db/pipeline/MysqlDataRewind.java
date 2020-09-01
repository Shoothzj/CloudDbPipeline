package com.github.shoothzj.db.pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.shoothzj.db.pipeline.module.DbInfoDto;
import com.github.shoothzj.db.pipeline.module.MysqlInfoDto;
import com.github.shoothzj.db.pipeline.module.RewindTaskDto;
import com.github.shoothzj.db.pipeline.module.TransformDto;
import com.github.shoothzj.db.pipeline.util.MysqlUtil;
import com.github.shoothzj.javatool.util.LogUtil;
import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlDataRewind {

    public static void main(String[] args) throws IOException {
        LogUtil.configureLog();
        final URL resourceUrl = Resources.getResource("rewind/mysql_sample.yaml");
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final List<RewindTaskDto> rewindTaskDtos = mapper.readValue(resourceUrl, new TypeReference<List<RewindTaskDto>>() {
        });
        log.info("rewind config dto is [{}]", rewindTaskDtos);
        //开始处理rewind task
        for (RewindTaskDto rewindTaskDto : rewindTaskDtos) {
            processRewindTask(rewindTaskDto);
        }
    }

    public static void processRewindTask(RewindTaskDto rewindTaskDto) {
        log.info("rewind task dto is [{}]", rewindTaskDto);
        final DbInfoDto dbInfo = rewindTaskDto.getDbInfo();
        if (dbInfo.getDbType().equals("mysql")) {
            processRewindMysql(rewindTaskDto.getDbInfo().getMysqlInfo(), rewindTaskDto.getTransform());
        } else {
            throw new IllegalArgumentException("Not supported db type yet");
        }
    }

    public static void processRewindMysql(MysqlInfoDto mysqlInfoDto, TransformDto transformDto) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(mysqlInfoDto.getJdbcUrl());
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        HikariDataSource dataSource = new HikariDataSource(config);

        final String tableName = mysqlInfoDto.getTableName();
        final String primaryKey = mysqlInfoDto.getPrimaryKey();
        long skip = -1;
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
                    log.info("size is [{}]", size);
                }
            } catch (Exception e) {
                log.error("exec exception ", e);
            }
        }
    }

    public static String firstQuerySql(String tableName, String primaryKey, long pageSize) {
        // 子查询分页 SELECT * FROM $TABLE_NAME
        // WHERE $PRIMARY_KEY >= (SELECT $PRIMARY_KEY FROM $TABLE_NAME ORDER BY $PRIMARY_KEY asc LIMIT ${(page-1)*pagesize} )
        // ORDER BY $PRIMARY_KEY asc LIMIT $PAGE_SIZE
        String phase1Sql = "SELECT * FROM " + tableName;
        String phase2Sql = " WHERE " + primaryKey + " >= (SELECT " + primaryKey + " FROM " + tableName;
        String phase3Sql = " ORDER BY " + primaryKey + " asc LIMIT 1 )";
        String phase4Sql = " ORDER BY " + primaryKey + " asc LIMIT " + pageSize;
        return phase1Sql + phase2Sql + phase3Sql + phase4Sql + ";";
    }

    public static String subQuerySql(String tableName, String primaryKey, long skip, long pageSize) {
        // 子查询分页 SELECT * FROM $TABLE_NAME
        // WHERE $PRIMARY_KEY >= (SELECT $PRIMARY_KEY FROM $TABLE_NAME ORDER BY $PRIMARY_KEY asc LIMIT ${(page-1)*pagesize} )
        // ORDER BY $PRIMARY_KEY asc LIMIT $PAGE_SIZE
        String phase1Sql = "SELECT * FROM " + tableName;
        String phase2Sql = " WHERE " + primaryKey + " >= (SELECT " + primaryKey + " FROM " + tableName;
        String phase3Sql = " ORDER BY " + primaryKey + " asc LIMIT " + skip + ",1 )";
        String phase4Sql = " ORDER BY " + primaryKey + " asc LIMIT " + pageSize;
        return phase1Sql + phase2Sql + phase3Sql + phase4Sql + ";";
    }

}
