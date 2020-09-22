package com.github.shoothzj.db.pipeline.api.exchange;

import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class StringExchange extends AbstractExchange {

    private String value;

    public StringExchange(String value) {
        this.value = value;
    }

    public String stringValue() {
        return value;
    }

}
