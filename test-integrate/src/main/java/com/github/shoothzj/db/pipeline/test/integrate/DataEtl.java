package com.github.shoothzj.db.pipeline.test.integrate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.shoothzj.db.pipeline.api.module.DbType;
import com.github.shoothzj.db.pipeline.api.module.EtlTaskDto;
import com.github.shoothzj.db.pipeline.api.module.ExtractDto;
import com.github.shoothzj.db.pipeline.api.module.db.DbInfoDto;
import com.github.shoothzj.db.pipeline.core.AbstractEtlWork;
import com.github.shoothzj.db.pipeline.core.AbstractLoad;
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
    public void startEtlTaskWithRelativePath(String relativePath) {
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
        final Object extractInfo;
        final Object loadInfo;
        final AbstractEtlWork<Object, Object, Object> abstractEtlWork;
        final AbstractLoad<Object> abstractLoad;
        {
            final Class<?> extractClass;
            final DbInfoDto extractDbInfo = extractDto.getDbInfo();
            if (extractDbInfo.getDbType().equals(DbType.Mysql)) {
                extractClass = Class.forName("com.github.shoothzj.db.pipeline.plugin.mysql8.MysqlDataEtlWork");
                extractInfo = extractDbInfo.getMysqlInfo();
            } else {
                throw new IllegalArgumentException("not supported yet.");
            }
            abstractEtlWork = (AbstractEtlWork<Object, Object, Object>) extractClass.newInstance();
        }
        {
            final Class<?> loadClass;
            final DbInfoDto loadDbInfo = etlTaskDto.getLoad().getDbInfo();
            if (loadDbInfo.getDbType().equals(DbType.Mongo)) {
                loadClass = Class.forName("com.github.shoothzj.db.pipeline.plugin.mongo.MongoDataLoad");
                loadInfo = loadDbInfo.getMongoInfo();
            } else {
                throw new IllegalArgumentException("not supported yet.");
            }
            abstractLoad = (AbstractLoad<Object>) loadClass.newInstance();
            abstractLoad.init(loadInfo);
        }
        abstractEtlWork.init(extractInfo, abstractLoad, etlTaskDto.getTransform());
        final boolean workResult = abstractEtlWork.work();
        log.info("rewind task end, work Result is [{}] cost time is [{}]", workResult, System.currentTimeMillis() - startTime);
    }

}
