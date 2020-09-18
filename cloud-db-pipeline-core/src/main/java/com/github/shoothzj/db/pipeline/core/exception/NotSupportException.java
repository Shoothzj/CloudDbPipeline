package com.github.shoothzj.db.pipeline.core.exception;

import com.github.shoothzj.db.pipeline.core.module.StageEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class NotSupportException extends IllegalArgumentException {

    public NotSupportException(StageEnum stageEnum, String typeDef) {
        super(String.format("Stage %s not supported yet. typeDef is %s", stageEnum, typeDef));
    }

}
