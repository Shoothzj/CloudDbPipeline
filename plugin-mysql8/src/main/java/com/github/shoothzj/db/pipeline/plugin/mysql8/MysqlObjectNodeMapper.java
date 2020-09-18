package com.github.shoothzj.db.pipeline.plugin.mysql8;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.api.module.transform.MapDto;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import com.github.shoothzj.db.pipeline.core.mapper.AbstractObjectNodeMapper;
import com.github.shoothzj.javatool.service.JacksonService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlObjectNodeMapper extends AbstractObjectNodeMapper<ResultSet> {

    private TransformDto transformDto;

    public MysqlObjectNodeMapper(TransformDto transformDto) {
        this.transformDto = transformDto;
    }

    @Override
    public ObjectNode map2ObjectNode(ResultSet resultSet) {
        ObjectNode objectNode = JacksonService.createObjectNode();
        List<MapDto> mapDtos = transformDto.getMap();
        for (MapDto mapDto : mapDtos) {
            String auxName = StringUtils.isNotEmpty(mapDto.getReName()) ? mapDto.getReName(): mapDto.getFieldName();
            try {
                Object object = resultSet.getObject(mapDto.getFieldName());
                if (object instanceof String) {
                    objectNode.put(auxName, (String) object);
                } else {
                    throw new IllegalStateException("not supported yet.");
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        return objectNode;
    }

}