package com.github.shoothzj.db.pipeline.api;

/**
 * @author hezhangjian
 */
public abstract class AbstractDataRewind {

    protected abstract long processRewind();

    protected abstract void close();

}