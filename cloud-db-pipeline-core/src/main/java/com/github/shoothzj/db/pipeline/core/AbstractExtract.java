package com.github.shoothzj.db.pipeline.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.api.IExtract;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author hezhangjian
 * S: 源数据库连接信息
 * PT: 主键数据类型
 */
@Slf4j
public abstract class AbstractExtract<S, PT> implements IExtract<PT> {

    public abstract void init(S s, TransformDto transformDto);

    @Override
    public abstract SourceBrief<PT> detectBrief();

    @Override
    public abstract List<ObjectNode> extract();

    @Override
    public abstract List<ObjectNode> extract(PT start, PT end);
}
