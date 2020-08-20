package com.github.shoothzj.db.pipeline.json.mysql8.module;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class FieldDescribe {

    private int columnType;

    private String columnTypeName;

    private int columnDisplaySize;

    private String columnLabel;

    public FieldDescribe() {
    }

}
