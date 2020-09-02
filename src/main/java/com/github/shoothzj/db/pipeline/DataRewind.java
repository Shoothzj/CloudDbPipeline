package com.github.shoothzj.db.pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.shoothzj.db.pipeline.module.DbInfoDto;
import com.github.shoothzj.db.pipeline.module.DbType;
import com.github.shoothzj.db.pipeline.module.RewindTaskDto;
import com.google.common.io.Resources;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class DataRewind {

    /**
     * @param absolutePath 相对地址
     */
    public void startRewindTaskWithAbsolutePath(String absolutePath) {
        final URL resourceUrl = Resources.getResource(absolutePath);
        this.startRewindTaskWithUrl(resourceUrl);
    }

    /**
     * @param resourceUrl YamlUrl地址
     */
    public void startRewindTaskWithUrl(URL resourceUrl) {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            final List<RewindTaskDto> rewindTaskDtos = mapper.readValue(resourceUrl, new TypeReference<List<RewindTaskDto>>() {
            });
            for (RewindTaskDto rewindTaskDto : rewindTaskDtos) {
                startRewindTask(rewindTaskDto);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startRewindTask(RewindTaskDto rewindTaskDto) {
        long startTime = System.currentTimeMillis();
        log.info("rewind task start, dto is [{}] start time is [{}]", rewindTaskDto, startTime);
        final DbInfoDto dbInfo = rewindTaskDto.getDbInfo();
        final long count;
        if (dbInfo.getDbType().equals(DbType.Mongo)) {
            MongoDataRewind mongoDataRewind = new MongoDataRewind(rewindTaskDto.getDbInfo().getMongoInfo(), rewindTaskDto.getTransform());
            count = mongoDataRewind.processRewind();
            mongoDataRewind.close();
        } else if (dbInfo.getDbType().equals(DbType.Mysql)) {
            MysqlDataRewind mysqlDataRewind = new MysqlDataRewind(rewindTaskDto.getDbInfo().getMysqlInfo(), rewindTaskDto.getTransform());
            count = mysqlDataRewind.processRewind();
            mysqlDataRewind.close();
        } else {
            throw new IllegalArgumentException("Not supported db type yet");
        }
        log.info("rewind task end, count is [{}] cost time is [{}]", count, System.currentTimeMillis() - startTime);
    }


}