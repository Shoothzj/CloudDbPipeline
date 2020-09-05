package com.github.shoothzj.db.pipeline.api.module.db;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class MongoInfoDto {

    private String connectionStr;

    private String dbName;

    private String collectionName;

    private String username;

    private String password;

    public MongoInfoDto() {
    }

}
