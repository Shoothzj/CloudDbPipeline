package com.github.shoothzj.db.pipeline.test.integrate;

import com.github.shoothzj.javatool.util.LogUtil;

/**
 * @author hezhangjian
 */
public class MongoDataRewindTest {

    public static void main(String[] args) {
        LogUtil.configureLog();
        DataRewind dataRewind = new DataRewind();
        dataRewind.startRewindTaskWithRelativePath("rewind/mongo_sample.yaml");
    }

}