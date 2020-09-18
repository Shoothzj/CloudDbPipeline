package com.github.shoothzj.db.pipeline.plugin.mongo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.shoothzj.db.pipeline.core.exception.NotSupportException;
import com.github.shoothzj.db.pipeline.core.mapper.AbstractGenericMapper;
import com.github.shoothzj.db.pipeline.core.module.StageEnum;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.Iterator;
import java.util.Map;

/**
 * @author hezhangjian
 */
@Slf4j
public class MongoDocumentMapper extends AbstractGenericMapper<Document> {


    @Override
    public Document map2Generic(ObjectNode objectNode) {
        final Document document = new Document();
        final Iterator<Map.Entry<String, JsonNode>> entryIterator = objectNode.fields();
        while (entryIterator.hasNext()) {
            final Map.Entry<String, JsonNode> jsonNodeEntry = entryIterator.next();
            put(document, jsonNodeEntry.getKey(), jsonNodeEntry.getValue());
        }
        return document;
    }

    private void put(Document document, String name, JsonNode value) {
        {
            if (value instanceof IntNode) {
                document.put(name, value.intValue());
                return;
            }
        }
        {
            if (value instanceof TextNode) {
                document.put(name, value.textValue());
                return;
            }
        }
        throw new NotSupportException(StageEnum.GENERIC_MAP, value.getClass().toString());
    }

}
