package com.github.shoothzj.db.pipeline.core.util;

import com.github.shoothzj.db.pipeline.core.exception.NotSupportException;
import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;
import com.github.shoothzj.db.pipeline.core.module.StageEnum;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

/**
 * @author hezhangjian
 */
@Slf4j
public class MapExchangeUtil {

    public static void putVal(MapExchange mapExchange, String name, Object object) {
        if (object instanceof String) {
            mapExchange.addExchange(name, (String) object);
        } else if (object instanceof Integer) {
            mapExchange.addExchange(name, (Integer) object);
        } else if (object instanceof Instant) {
            mapExchange.addExchange(name, (Instant) object);
        } else {
            throw new NotSupportException(StageEnum.SOURCE_MAPPER, object.getClass().toString());
        }
    }

}
