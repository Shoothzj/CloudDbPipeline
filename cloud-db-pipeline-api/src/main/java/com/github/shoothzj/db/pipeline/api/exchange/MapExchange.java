package com.github.shoothzj.db.pipeline.api.exchange;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hezhangjian
 */
@Slf4j
public class MapExchange extends AbstractExchange {

    private final Map<String, AbstractExchange> exchangeMap = new HashMap<>();

    public MapExchange() {
    }

    public void addExchange(String name, Integer value) {
        exchangeMap.put(name, new IntExchange(value));
    }

    public void addExchange(String name, Long value) {
        exchangeMap.put(name, new LongExchange(value));
    }

    public void addExchange(String name, String value) {
        exchangeMap.put(name, new StringExchange(value));
    }

    public void addExchange(String name, Instant value) {
        exchangeMap.put(name, new InstantExchange(value));
    }

    public void addExchange(String name, AbstractExchange exchange) {
        exchangeMap.put(name, exchange);
    }

    public AbstractExchange getExchange(String name) {
        return exchangeMap.get(name);
    }

    public void traverse(ExchangeTraverseCallback callback) {
        exchangeMap.entrySet().forEach(entry -> callback.callback(entry.getKey(), entry.getValue()));
    }

    public Map<String, AbstractExchange> getExchangeMap() {
        return exchangeMap;
    }
}
