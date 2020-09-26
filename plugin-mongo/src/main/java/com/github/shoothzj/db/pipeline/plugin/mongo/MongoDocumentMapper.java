package com.github.shoothzj.db.pipeline.plugin.mongo;

import com.github.shoothzj.db.pipeline.api.exchange.AbstractExchange;
import com.github.shoothzj.db.pipeline.api.exchange.InstantExchange;
import com.github.shoothzj.db.pipeline.api.exchange.IntExchange;
import com.github.shoothzj.db.pipeline.api.exchange.LongExchange;
import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;
import com.github.shoothzj.db.pipeline.api.exchange.StringExchange;
import com.github.shoothzj.db.pipeline.core.exception.NotSupportException;
import com.github.shoothzj.db.pipeline.core.mapper.AbstractGenericMapper;
import com.github.shoothzj.db.pipeline.core.module.StageEnum;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

/**
 * @author hezhangjian
 */
@Slf4j
public class MongoDocumentMapper extends AbstractGenericMapper<Document> {


    @Override
    public Document map2Generic(MapExchange mapExchange) {
        final Document document = new Document();
        mapExchange.traverse((name, abstractExchange) -> put(document, name, abstractExchange));
        return document;
    }

    private void put(Document document, String name, AbstractExchange value) {
        if (name.contains(".")) {
            throw new NotSupportException(StageEnum.LOAD, "mongo load does not support dot yet.");
        }
        {
            if (value instanceof IntExchange) {
                document.put(name, ((IntExchange) value).intValue());
                return;
            }
        }
        {
            if (value instanceof LongExchange) {
                document.put(name, ((LongExchange) value).longValue());
                return;
            }
        }
        {
            if (value instanceof StringExchange) {
                document.put(name, ((StringExchange) value).stringValue());
                return;
            }
        }
        {
            if (value instanceof InstantExchange) {
                document.put(name, ((InstantExchange) value).instantValue());
                return;
            }
        }
        throw new NotSupportException(StageEnum.GENERIC_MAP, value.getClass().toString());
    }

}
