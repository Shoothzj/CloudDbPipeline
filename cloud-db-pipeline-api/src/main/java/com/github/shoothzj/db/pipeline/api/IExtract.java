package com.github.shoothzj.db.pipeline.api;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;

import java.util.List;

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
    List<ObjectNode> extract();

    /**
     * 提取start~end的信息
     *
     * @param start
     * @param end
     * @return
     */
    List<ObjectNode> extract(PT start, PT end);

}
