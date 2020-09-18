package com.github.shoothzj.db.pipeline.core.util;

import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.shoothzj.db.pipeline.api.module.transform.MapDto;
import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class JsonCalculateUtil {

    /**
     * 返回字符串类型，则是字符格式
     *
     * @param source Object类型，需要是满足运算的基本格式
     *               现在支持String类型
     * @param mapDto
     * @return
     */
    public static Object processMap(Object source, MapDto mapDto) {
        if (source instanceof IntNode) {
            IntNode intNode = (IntNode) source;
            return CalculateUtil.processMap(intNode.intValue(), mapDto);
        }
        if (source instanceof TextNode) {
            TextNode textNode = (TextNode) source;
            return CalculateUtil.processMap(textNode.textValue(), mapDto);
        }
        throw new IllegalStateException("not implemented yet.");
    }

}
