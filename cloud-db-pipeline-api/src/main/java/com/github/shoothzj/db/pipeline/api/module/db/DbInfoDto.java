package com.github.shoothzj.db.pipeline.api.module.db;

import com.github.shoothzj.db.pipeline.api.module.DbType;
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
