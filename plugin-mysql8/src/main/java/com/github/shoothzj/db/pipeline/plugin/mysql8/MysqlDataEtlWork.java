package com.github.shoothzj.db.pipeline.plugin.mysql8;

import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;
import com.github.shoothzj.db.pipeline.api.module.db.MysqlInfoDto;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import com.github.shoothzj.db.pipeline.core.AbstractEtlWork;
import com.github.shoothzj.db.pipeline.core.AbstractLoad;
import com.github.shoothzj.db.pipeline.core.mapper.AbstractMapExchangeMapper;
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
public class MysqlDataEtlWork<PT, D> extends AbstractEtlWork<MysqlInfoDto, D, PT> {

    private MysqlInfoDto mysqlInfoDto;

    private TransformDto transformDto;

    private HikariDataSource dataSource;

    private String tableName;

    private String primaryKey;

    private AbstractMapExchangeMapper<ResultSet> exchangeMapper;

    private AbstractLoad<D> load;

    @Override
    public void init(MysqlInfoDto mysqlInfoDto, AbstractLoad<D> load, TransformDto transformDto) {
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
        this.load = load;
        exchangeMapper = new MysqlMapExchangeMapper(this.transformDto);
    }

    @Override
    public SourceBrief<PT> detectBrief() {
        return null;
    }

    @Override
    public boolean work() {
        long skip = 0;
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
                List<MapExchange> resultList = new ArrayList<>();
                final ResultSet resultSet = prepareStatement.executeQuery();
                int size = 0;
                while (resultSet.next()) {
                    final MapExchange mapExchange = exchangeMapper.map2MapExchange(resultSet);
                    resultList.add(transform(mapExchange, transformDto));
                    size++;
                }
                if (size == 0) {
                    break;
                }
                load.load(resultList);
                skip += size;
                log.info("size is [{}]", size);
            } catch (Exception e) {
                log.error("exec exception ", e);
            }
        }
        return true;
    }

    @Override
    public boolean work(PT start, PT end) {
        return false;
    }

}