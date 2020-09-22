package com.github.shoothzj.db.pipeline.api;

import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;

import java.util.List;

/**
 * @author hezhangjian
 */
public interface ILoad {

    /**
     * 存入一个 MapExchange
     * @param mapExchange
     * @return
     */
    boolean load(MapExchange mapExchange);

    /**
     * 存入 MapExchangeList
     * @param mapExchanges
     * @return
     */
    boolean load(List<MapExchange> mapExchanges);

}
