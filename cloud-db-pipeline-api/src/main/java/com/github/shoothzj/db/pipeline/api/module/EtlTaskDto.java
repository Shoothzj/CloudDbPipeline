package com.github.shoothzj.db.pipeline.api.module;

import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class EtlTaskDto {

    private String taskName;

    private ExtractDto extract;

    private TransformDto transform;

    private LoadDto load;

}
