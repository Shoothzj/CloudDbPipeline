package com.github.shoothzj.db.pipeline.api.module.transform;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
@Data
public class TransformDto {

    private List<MapDto> map;

}
