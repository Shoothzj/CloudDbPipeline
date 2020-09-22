package com.github.shoothzj.db.pipeline.api.exchange;

import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class BooleanExchange extends AbstractExchange {

    private boolean value;

    public BooleanExchange(boolean value) {
        this.value = value;
    }

    public boolean booleanValue() {
        return value;
    }

}
