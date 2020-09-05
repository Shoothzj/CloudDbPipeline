package com.github.shoothzj.db.pipeline.test.integrate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.shoothzj.db.pipeline.core.AbstractRewind;
import com.github.shoothzj.db.pipeline.api.module.DbType;
import com.github.shoothzj.db.pipeline.api.module.RewindTaskDto;
import com.github.shoothzj.db.pipeline.api.module.db.DbInfoDto;
import com.github.shoothzj.javatool.util.YamlUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class DataRewind {

    /**
     * @param relativePath 相对地址
     */
    public void startRewindTaskWithRelativePath(String relativePath) {
        final List<RewindTaskDto> rewindTaskDtos = YamlUtil.relativePathToList(relativePath, new TypeReference<List<RewindTaskDto>>() {
        });
        for (RewindTaskDto rewindTaskDto : rewindTaskDtos) {
            try {
                startRewindTask(rewindTaskDto);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void startRewindTask(RewindTaskDto rewindTaskDto) throws Exception {
        long startTime = System.currentTimeMillis();
        log.info("rewind task start, dto is [{}] start time is [{}]", rewindTaskDto, startTime);
        final DbInfoDto dbInfo = rewindTaskDto.getDbInfo();
        final Class<?> aClass;
        final Object exactlyInfo;
        if (dbInfo.getDbType().equals(DbType.Mongo)) {
            aClass = Class.forName("com.github.shoothzj.db.pipeline.plugin.mongo.MongoDataRewind");
            exactlyInfo = dbInfo.getMongoInfo();
        } else if (dbInfo.getDbType().equals(DbType.Mysql)) {
            aClass = Class.forName("com.github.shoothzj.db.pipeline.plugin.mysql8.Mysql8DataRewind");
            exactlyInfo = dbInfo.getMysqlInfo();
        } else {
            throw new IllegalArgumentException("Not supported db type yet");
        }
        final AbstractRewind<Object, String> abstractDataRewind = (AbstractRewind<Object, String>) aClass.newInstance();
        abstractDataRewind.init(exactlyInfo, rewindTaskDto.getTransform());
        final boolean work = abstractDataRewind.work();
        abstractDataRewind.close();
        log.info("rewind task end, count is [{}] cost time is [{}]", work, System.currentTimeMillis() - startTime);
    }


}