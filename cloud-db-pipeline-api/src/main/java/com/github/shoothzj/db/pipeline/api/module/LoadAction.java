package com.github.shoothzj.db.pipeline.api.module;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class LoadAction {

    private String fieldName;

    private LoadConvert loadConvert;

    public LoadAction() {
    }
}
