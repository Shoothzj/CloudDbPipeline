package com.github.shoothzj.db.pipeline.api;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * @author hezhangjian
 */
public interface ILoad {

    /**
     * 存入一个ObjectNode
     * @param objectNode
     * @return
     */
    boolean load(ObjectNode objectNode);

    /**
     * 存入ObjectNodeList
     * @param objectNodeList
     * @return
     */
    boolean load(List<ObjectNode> objectNodeList);

}
