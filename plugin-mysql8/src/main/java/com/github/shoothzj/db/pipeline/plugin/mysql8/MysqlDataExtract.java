package com.github.shoothzj.db.pipeline.plugin.mysql8;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;
import com.github.shoothzj.db.pipeline.api.module.db.MysqlInfoDto;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import com.github.shoothzj.db.pipeline.core.AbstractExtract;
import com.github.shoothzj.db.pipeline.core.mapper.AbstractObjectNodeMapper;
import com.github.shoothzj.db.pipeline.plugin.mysql8.util.MysqlConcatUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlDataExtract<PT> extends AbstractExtract<MysqlInfoDto, PT> {

    private MysqlInfoDto mysqlInfoDto;

    private TransformDto transformDto;

    private HikariDataSource dataSource;

    private String tableName;

    private String primaryKey;

    private AbstractObjectNodeMapper<ResultSet> objectNodeMapper;

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
        objectNodeMapper = new MysqlObjectNodeMapper(this.transformDto);
    }

    @Override
    public SourceBrief<PT> detectBrief() {
        return null;
    }

    @Override
    public List<ObjectNode> extract() {
        long skip = 0;
        List<ObjectNode> resultList = new ArrayList<>();
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
                    resultList.add(objectNodeMapper.map2ObjectNode(resultSet));
                    size++;
                }
                if (size == 0) {
                    break;
                }
                skip += size;
                log.info("size is [{}]", size);
            } catch (Exception e) {
                log.error("exec exception ", e);
            }
        }
        return resultList;
    }

    @Override
    public List<ObjectNode> extract(PT start, PT end) {
        return null;
    }

    private ObjectNode processSingleItem(ResultSet resultSet) {
        return null;
    }


}