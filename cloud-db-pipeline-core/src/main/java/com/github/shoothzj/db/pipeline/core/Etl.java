package com.github.shoothzj.db.pipeline.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.api.IExtract;
import com.github.shoothzj.db.pipeline.api.ILoad;
import com.github.shoothzj.db.pipeline.api.IWork;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hezhangjian
 * S: 源数据库连接信息
 * D: 目标数据库连接信息
 * PT: 主键数据类型
 */
@Slf4j
public class Etl<S, D, PT> extends AbstractTransform implements IWork<PT>, IExtract<PT>, ILoad {

    private AbstractExtract<S, PT> extract;

    private AbstractLoad<D> load;

    private TransformDto transformDto;

    public void initEtl(S s, D d, AbstractExtract<S, PT> extract, AbstractLoad<D> load, TransformDto transformDto) {
        this.extract = extract;
        this.load = load;
        this.transformDto = transformDto;
        extract.init(s, transformDto);
        load.init(d);
    }

    @Override
    public SourceBrief<PT> detectBrief() {
        return extract.detectBrief();
    }

    @Override
    public boolean work() {
        final List<ObjectNode> objectNodeList = extract();
        final List<ObjectNode> nodes = objectNodeList.stream()
                .map(objectNode -> transform(objectNode, transformDto)).collect(Collectors.toList());
        return load(nodes);
    }

    @Override
    public boolean work(PT start, PT end) {
        final List<ObjectNode> objectNodeList = extract(start, end);
        final List<ObjectNode> nodes = objectNodeList.stream()
                .map(objectNode -> transform(objectNode, transformDto)).collect(Collectors.toList());
        return load(nodes);
    }

    @Override
    public List<ObjectNode> extract() {
        return extract.extract();
    }

    @Override
    public List<ObjectNode> extract(PT start, PT end) {
        return extract.extract(start, end);
    }

    @Override
    public boolean load(ObjectNode objectNode) {
        return load.load(objectNode);
    }

    @Override
    public boolean load(List<ObjectNode> objectNodeList) {
        return load.load(objectNodeList);
    }

}
