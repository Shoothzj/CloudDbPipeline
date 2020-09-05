package com.github.shoothzj.db.pipeline.api;

import com.github.shoothzj.db.pipeline.api.module.SourceBrief;

/**
 * @author hezhangjian
 */
public interface IDetect<PT> {

    /**
     * 检查简要信息
     * @return
     */
    SourceBrief<PT> detectBrief();

}
