package com.github.shoothzj.db.pipeline.core;

import com.github.shoothzj.db.pipeline.api.ILoad;
import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author hezhangjian
 * D: 目标数据库连接信息
 */
@Slf4j
public abstract class AbstractLoad<D> implements ILoad {

    public abstract void init(D d);

    @Override
    public abstract boolean load(MapExchange mapExchange);

    @Override
    public abstract boolean load(List<MapExchange> mapExchanges);

}
