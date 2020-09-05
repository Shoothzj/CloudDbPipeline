package com.github.shoothzj.db.pipeline.plugin.mongo.util;

import com.github.shoothzj.db.pipeline.api.module.transform.MapDto;
import com.github.shoothzj.db.pipeline.core.util.CalculateUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class MongoCalculateUtil {

    public static Document mapDocument(Document document, List<MapDto> mapDtos) {
        for (MapDto mapDto : mapDtos) {
            final String fieldName = mapDto.getFieldName();
            final Object sourceObject = document.get(fieldName);
            final Object returnObj = CalculateUtil.processMap(sourceObject, mapDto);
            putValue(document, fieldName, returnObj);
        }
        return document;
    }

    public static void putValue(Document document, String fieldName, Object value) {
        if (!fieldName.contains(".")) {
            document.put(fieldName, value);
            return;
        }
        final String[] split = fieldName.split("\\.");
        Document aux = document;
        for (int i = 0; i < split.length - 1; i++) {
            if (aux.get(split[i]) == null) {
                aux.put(split[i], new Document());
            }
            aux = (Document) aux.get(split[i]);
        }
        aux.put(split[split.length - 1], value);
    }

}
