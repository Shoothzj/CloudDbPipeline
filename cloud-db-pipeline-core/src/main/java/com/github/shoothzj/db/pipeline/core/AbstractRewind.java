package com.github.shoothzj.db.pipeline.core;

import com.github.shoothzj.db.pipeline.api.IWork;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;

/**
 * @author hezhangjian
 * S: 源数据库连接信息
 * PT: 主键数据类型
 */
public abstract class AbstractRewind<S, PT> extends AbstractTransform implements IWork<PT> {

    public abstract void init(S s, TransformDto transformDto);

    @Override
    public abstract SourceBrief<PT> detectBrief();

    @Override
    public boolean work() {
        return rewind();
    }

    @Override
    public boolean work(PT start, PT end) {
        return rewind(start, end);
    }

    /**
     * 提取全部信息
     *
     * @return
     */
    public abstract boolean rewind();

    /**
     * 提取start~end的信息
     *
     * @param start
     * @param end
     * @return
     */
    public abstract boolean rewind(PT start, PT end);

    public abstract void close();

}
