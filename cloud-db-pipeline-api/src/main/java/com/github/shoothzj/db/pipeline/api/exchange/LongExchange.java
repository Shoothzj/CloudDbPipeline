package com.github.shoothzj.db.pipeline.api.exchange;

/**
 * @author hezhangjian
 */
public class LongExchange extends AbstractExchange{

    private long value;

    public LongExchange(long value) {
        this.value = value;
    }

    public long longValue() {
        return value;
    }

}
