package com.github.shoothzj.db.pipeline.api.module.transform;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class MapDto {

    private String fieldName;

    private TransformType transformType;

    private FunctionInfo functionInfo;

    private ConstantInfo constantInfo;

}
