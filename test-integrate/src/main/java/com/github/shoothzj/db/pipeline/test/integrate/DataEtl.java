package com.github.shoothzj.db.pipeline.test.integrate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.shoothzj.db.pipeline.api.module.EtlTaskDto;
import com.github.shoothzj.db.pipeline.api.module.ExtractDto;
import com.github.shoothzj.db.pipeline.api.module.LoadDto;
import com.github.shoothzj.javatool.util.YamlUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class DataEtl {

    /**
     * @param relativePath 相对地址
     */
    public void startRewindTaskWithRelativePath(String relativePath) {
        final List<EtlTaskDto> etlTaskDtos = YamlUtil.relativePathToList(relativePath, new TypeReference<List<EtlTaskDto>>() {
        });
        for (EtlTaskDto etlTaskDto : etlTaskDtos) {
            try {
                startEtlTask(etlTaskDto);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void startEtlTask(EtlTaskDto etlTaskDto) throws Exception {
        long startTime = System.currentTimeMillis();
        log.info("rewind task start, dto is [{}] start time is [{}]", etlTaskDto, startTime);
        final ExtractDto extractDto = etlTaskDto.getExtract();
        log.info("extract dto is [{}]", extractDto);
        final LoadDto loadDto = etlTaskDto.getLoad();
        log.info("load dto is [{}]", loadDto);
        log.info("rewind task end, cost time is [{}]", System.currentTimeMillis() - startTime);
    }

}
