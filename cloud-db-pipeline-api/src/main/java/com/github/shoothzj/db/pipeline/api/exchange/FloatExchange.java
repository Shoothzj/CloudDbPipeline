package com.github.shoothzj.db.pipeline.api.exchange;

/**
 * @author hezhangjian
 */
public class FloatExchange extends AbstractExchange {

    private float value;

    public FloatExchange(float value) {
        this.value = value;
    }

    public float floatValue() {
        return value;
    }

}
