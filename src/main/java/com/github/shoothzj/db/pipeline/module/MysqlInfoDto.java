package com.github.shoothzj.db.pipeline.module;

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
