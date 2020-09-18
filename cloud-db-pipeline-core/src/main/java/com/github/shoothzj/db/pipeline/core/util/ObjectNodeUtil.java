package com.github.shoothzj.db.pipeline.core.util;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.core.exception.NotSupportException;
import com.github.shoothzj.db.pipeline.core.module.StageEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class ObjectNodeUtil {

    public static void putVal(ObjectNode objectNode, String name, Object object) {
        if (object instanceof String) {
            objectNode.put(name, (String) object);
        } else if (object instanceof Integer) {
            objectNode.put(name, (Integer) object);
        } else {
            throw new NotSupportException(StageEnum.SOURCE_MAPPER, object.getClass().toString());
        }
    }

}
