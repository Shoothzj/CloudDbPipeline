package com.github.shoothzj.db.pipeline.plugin.mysql8;

import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;
import com.github.shoothzj.db.pipeline.api.module.transform.MapDto;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import com.github.shoothzj.db.pipeline.core.mapper.AbstractMapExchangeMapper;
import com.github.shoothzj.db.pipeline.core.util.MapExchangeUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlMapExchangeMapper extends AbstractMapExchangeMapper<ResultSet> {

    private final TransformDto transformDto;

    public MysqlMapExchangeMapper(TransformDto transformDto) {
        this.transformDto = transformDto;
    }

    @Override
    public MapExchange map2MapExchange(ResultSet resultSet) {
        final MapExchange mapExchange = new MapExchange();
        List<MapDto> mapDtos = transformDto.getMap();
        for (MapDto mapDto : mapDtos) {
            try {
                Object object = resultSet.getObject(mapDto.getFieldName());
                MapExchangeUtil.putVal(mapExchange, mapDto.getFieldName(), object);
            } catch (SQLException throwable) {
                log.error("sql exception is ", throwable);
            }
        }
        return mapExchange;
    }
}