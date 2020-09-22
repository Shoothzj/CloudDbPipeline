package com.github.shoothzj.db.pipeline.api.module.db;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class EsInfoDto {

    private String esHost;

    private String esDomain;

    private int esPort;

    private String schema;

    private String indexName;

    public EsInfoDto() {
    }
}
