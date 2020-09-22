package com.github.shoothzj.db.pipeline.core.mapper;

import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;

/**
 * @author hezhangjian
 */
public abstract class AbstractMapExchangeMapper<T> {

    public abstract MapExchange map2MapExchange(T t);

}
