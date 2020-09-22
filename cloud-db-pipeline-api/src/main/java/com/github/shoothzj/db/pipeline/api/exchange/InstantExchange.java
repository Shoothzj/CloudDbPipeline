package com.github.shoothzj.db.pipeline.api.exchange;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

/**
 * @author hezhangjian
 */
@Slf4j
public class InstantExchange extends AbstractExchange {

    private Instant value;

    public InstantExchange(Instant value) {
        this.value = value;
    }

    public Instant instantValue() {
        return value;
    }

}
