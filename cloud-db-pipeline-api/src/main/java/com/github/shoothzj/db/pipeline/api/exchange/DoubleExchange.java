package com.github.shoothzj.db.pipeline.api.exchange;

import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class DoubleExchange extends AbstractExchange {

    private double value;

    public DoubleExchange(double value) {
        this.value = value;
    }

    public double doubleValue() {
        return value;
    }

}
