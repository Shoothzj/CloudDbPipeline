package com.github.shoothzj.db.pipeline.core.module;

/**
 * @author hezhangjian
 */
public enum StageEnum {

    /**
     * Source to MapExchange的流程
     */
    SOURCE_MAPPER,
    /**
     * 泛型的map
     */
    GENERIC_MAP,

    /**
     * 函数运算
     */
    FUNCTION,

    /**
     * load到数据源过程
     */
    LOAD

}
