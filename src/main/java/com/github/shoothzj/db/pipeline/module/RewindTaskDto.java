package com.github.shoothzj.db.pipeline.module;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
@Data
public class RewindTaskDto {

    private String taskName;

    private DbInfoDto dbInfo;

    private TransformDto transform;

}
