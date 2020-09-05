package com.github.shoothzj.db.pipeline.core.util;

import com.github.shoothzj.db.pipeline.api.module.transform.FunctionInfo;
import com.github.shoothzj.db.pipeline.api.module.transform.FunctionName;
import com.github.shoothzj.db.pipeline.api.module.transform.MapDto;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformType;
import com.github.shoothzj.javatool.util.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * @author hezhangjian
 */
@Slf4j
public class CalculateUtil {

    /**
     * 返回字符串类型，则是字符格式
     *
     * @param source Object类型，需要是满足运算的基本格式
     *               现在支持String类型
     * @param mapDto
     * @return
     */
    public static Object processMap(Object source, MapDto mapDto) {
        if (source instanceof String) {
            String aux = (String) source;
            return processMap(aux, mapDto);
        }
        throw new IllegalStateException("not implemented yet.");
    }

    public static Object processMap(String source, MapDto mapDto) {
        final TransformType transformType = mapDto.getTransformType();
        if (transformType.equals(TransformType.Function)) {
            final FunctionInfo functionInfo = mapDto.getFunctionInfo();
            if (functionInfo.getFunctionName().equals(FunctionName.UUID)) {
                return UUID.randomUUID().toString();
            }
            if (functionInfo.getFunctionName().equals(FunctionName.Reverse)) {
                return StringUtil.reverse(source);
            } else {
                throw new IllegalArgumentException("not implement yet.");
            }
        } else if (transformType.equals(TransformType.Constant)) {
            return mapDto.getConstantInfo().getValue();
        } else {
            throw new IllegalArgumentException("not implement yet.");
        }
    }

}
