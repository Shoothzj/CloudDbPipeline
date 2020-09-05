package com.github.shoothzj.db.pipeline.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;

/**
 * @author hezhangjian
 */
public class AbstractTransform {

    protected ObjectNode transform(ObjectNode objectNode, TransformDto transformDto) {
        return null;
    }

}
