package com.github.shoothzj.db.pipeline.test.integrate;

import com.github.shoothzj.javatool.util.LogUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class Mysql2MongoScene1Test {

    public static void main(String[] args) {
        LogUtil.configureLog();
        final DataEtl dataEtl = new DataEtl();
        dataEtl.startEtlTaskWithRelativePath("etl/mysql_mongo_scene1.yaml");
    }

}
