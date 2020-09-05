package com.github.shoothzj.db.pipeline.core.mapper;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author hezhangjian
 */
public abstract class AbstractGenericMapper<T> {

    protected abstract T map2Generic(ObjectNode objectNode);

}
