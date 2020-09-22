package com.github.shoothzj.db.pipeline.api.exchange;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class ListExchange extends AbstractExchange {

    private final List<AbstractExchange> exchanges = new ArrayList<>();

    public ListExchange() {
    }

    public void addExchange(AbstractExchange exchange) {
        exchanges.add(exchange);
    }

    public List<AbstractExchange> getExchanges() {
        return exchanges;
    }
}
