package com.github.shoothzj.db.pipeline.api;

import com.github.shoothzj.db.pipeline.api.module.SourceBrief;

/**
 * @author hezhangjian
 */
public interface IWork<PT> extends IDetect<PT> {

    /**
     * 检查简要信息
     * @return
     */
    @Override
    SourceBrief<PT> detectBrief();

    /**
     * 全部信息
     *
     * @return
     */
    boolean work();

    /**
     * start~end的信息
     *
     * @param start
     * @param end
     * @return
     */
    boolean work(PT start, PT end);

}
