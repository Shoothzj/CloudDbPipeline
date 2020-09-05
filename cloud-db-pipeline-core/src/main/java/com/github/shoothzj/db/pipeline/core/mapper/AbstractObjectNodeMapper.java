package com.github.shoothzj.db.pipeline.core.mapper;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author hezhangjian
 */
public abstract class AbstractObjectNodeMapper<T> {

    protected abstract ObjectNode map2ObjectNode(T t);

}
