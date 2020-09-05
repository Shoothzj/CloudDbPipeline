package com.github.shoothzj.db.pipeline.api.module.db;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class MysqlInfoDto {

    private String jdbcUrl;

    private String tableName;

    private String username;

    private String password;

    private String primaryKey;

}
