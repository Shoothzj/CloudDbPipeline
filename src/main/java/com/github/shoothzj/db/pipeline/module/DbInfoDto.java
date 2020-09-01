package com.github.shoothzj.db.pipeline.module;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class DbInfoDto {

    private String dbType;

    private MongoInfoDto mongoInfo;

    private MysqlInfoDto mysqlInfo;

}
