package com.github.shoothzj.db.pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.shoothzj.db.pipeline.module.EtlTaskDto;
import com.github.shoothzj.javatool.util.LogUtil;
import com.google.common.io.Resources;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class MongoDataEtl {

    public static void main(String[] args) throws Exception {
        LogUtil.configureLog();
        final URL resourceUrl = Resources.getResource("etl_sample.yaml");
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final List<EtlTaskDto> etlTaskDtos = mapper.readValue(resourceUrl, new TypeReference<List<EtlTaskDto>>() {
        });
        log.info("etl config dto is [{}]", etlTaskDtos);
    }

}
