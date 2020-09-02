package com.github.shoothzj.db.pipeline.api;

/**
 * @author hezhangjian
 * 泛型T，是查询出来的类型
 */
public abstract class AbstractDataRewind<T> {

    /**
     * 处理整个Rewind
     * @return
     */
    protected abstract long processRewind();

    /**
     * 处理单个数据
     * @param t
     */
    protected abstract void processSingleItem(T t);

    /**
     * 关闭这个dataRewind对象
     */
    protected abstract void close();

}