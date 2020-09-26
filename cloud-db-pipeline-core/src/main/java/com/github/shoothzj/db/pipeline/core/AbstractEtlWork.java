package com.github.shoothzj.db.pipeline.core;

import com.github.shoothzj.db.pipeline.api.IExtract;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 * S: 源数据库连接信息
 * D: 目标数据库连接信息
 * PT: 主键数据类型
 */
@Slf4j
public abstract class AbstractEtlWork<S, D, PT> extends AbstractTransform implements IExtract<PT> {

    public abstract void init(S s, AbstractLoad<D> load, TransformDto transformDto);

    @Override
    public abstract SourceBrief<PT> detectBrief();

    @Override
    public abstract boolean work();

    @Override
    public abstract boolean work(PT start, PT end);
}
