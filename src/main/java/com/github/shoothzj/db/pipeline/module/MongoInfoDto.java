package com.github.shoothzj.db.pipeline.module;

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
