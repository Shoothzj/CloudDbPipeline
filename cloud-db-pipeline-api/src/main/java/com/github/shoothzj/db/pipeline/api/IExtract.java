package com.github.shoothzj.db.pipeline.api;

import com.github.shoothzj.db.pipeline.api.module.SourceBrief;

/**
 * @author hezhangjian
 * PT 主键类型
 */
public interface IExtract<PT> extends IDetect<PT> {

    @Override
    SourceBrief<PT> detectBrief();

    /**
     * 提取全部信息
     *
     * @return
     */
    boolean work();

    /**
     * 提取start~end的信息
     *
     * @param start
     * @param end
     * @return
     */
    boolean work(PT start, PT end);

}
