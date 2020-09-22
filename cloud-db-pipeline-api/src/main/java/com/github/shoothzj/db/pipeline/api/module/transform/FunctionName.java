package com.github.shoothzj.db.pipeline.api.module.transform;

import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public enum FunctionName {

    /**
     * UUID
     */
    UUID,
    /**
     * REVERSE
     */
    Reverse,

    /**
     * 转换为Java的Instant
     */
    UnixTimestampToInstant

}
