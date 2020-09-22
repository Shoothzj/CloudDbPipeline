package com.github.shoothzj.db.pipeline.api.exchange;

import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class IntExchange extends AbstractExchange {

    private int value;

    public IntExchange(int value) {
        this.value = value;
    }

    public int intValue() {
        return value;
    }

}
