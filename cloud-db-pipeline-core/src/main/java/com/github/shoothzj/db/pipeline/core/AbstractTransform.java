package com.github.shoothzj.db.pipeline.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.api.module.transform.MapDto;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import com.github.shoothzj.db.pipeline.core.util.CalculateUtil;
import com.github.shoothzj.db.pipeline.core.util.JsonCalculateUtil;
import com.github.shoothzj.db.pipeline.core.util.ObjectNodeUtil;
import com.github.shoothzj.javatool.service.JacksonService;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author hezhangjian
 */
public class AbstractTransform {

    protected ObjectNode transform(ObjectNode objectNode, TransformDto transformDto) {
        final List<MapDto> mapDtos = transformDto.getMap();
        final ObjectNode resultNode = JacksonService.createObjectNode();
        for (MapDto mapDto : mapDtos) {
            final String fieldName = mapDto.getFieldName();
            final Object processMap = JsonCalculateUtil.processMap(objectNode.get(fieldName), mapDto);
            String auxName = StringUtils.isNotEmpty(mapDto.getReName()) ? mapDto.getReName() : mapDto.getFieldName();
            ObjectNodeUtil.putVal(resultNode, auxName, processMap);
        }
        return resultNode;
    }

}
