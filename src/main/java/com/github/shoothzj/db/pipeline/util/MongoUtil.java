package com.github.shoothzj.db.pipeline.util;

import com.github.shoothzj.db.pipeline.module.MapDto;
import com.github.shoothzj.db.pipeline.module.TransformDto;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.List;
import java.util.UUID;

/**
 * @author hezhangjian
 */
@Slf4j
public class MongoUtil {

    public static void rewindDocument(MongoCollection<Document> collection, Document document, TransformDto transformDto) {
        final Document mapDocument = mapDocument(document, transformDto.getMap());
        collection.replaceOne(Filters.eq("_id", mapDocument.get("_id")), mapDocument);
    }

    public static Document mapDocument(Document document, List<MapDto> mapDtos) {
        for (MapDto mapDto : mapDtos) {
            final String fieldName = mapDto.getFieldName();
            final String transformType = mapDto.getTransformType();
            final String value = mapDto.getValue();
            log.debug("document is [{}] key is [{}] transformType is [{}] value is [{}]", document, fieldName, transformType, value);
            final Object sourceObject = document.get(fieldName);
            if (transformType.equals("function")) {
                if (value.equals("UUID")) {
                    putValue(document, mapDto.getFieldName(), UUID.randomUUID().toString());
                } else {
                    throw new IllegalArgumentException("not implement yet.");
                }
            } else if (transformType.equals("constant")) {
                putValue(document, mapDto.getFieldName(), value);
            } else {
                throw new IllegalArgumentException("not implement yet.");
            }
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
