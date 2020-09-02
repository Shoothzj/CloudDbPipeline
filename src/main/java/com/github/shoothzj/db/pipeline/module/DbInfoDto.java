package com.github.shoothzj.db.pipeline.module;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class DbInfoDto {

    private DbType dbType;

    private MongoInfoDto mongoInfo;

    private MysqlInfoDto mysqlInfo;

}
