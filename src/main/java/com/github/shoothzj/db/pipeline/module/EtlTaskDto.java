package com.github.shoothzj.db.pipeline.module;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class EtlTaskDto {

    private String taskName;

    private ExtractDto extract;

    private Object transform;

    private Object load;

}
