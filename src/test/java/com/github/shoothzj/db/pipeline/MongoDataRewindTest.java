package com.github.shoothzj.db.pipeline;

import com.github.shoothzj.javatool.util.LogUtil;
import org.junit.Test;

/**
 * @author hezhangjian
 */
public class MongoDataRewindTest {

    @Test
    public void main() {
        LogUtil.configureLog();
        LogUtil.configureLog();
        DataRewind dataRewind = new DataRewind();
        dataRewind.startRewindTaskWithAbsolutePath("rewind/mongo_sample.yaml");
    }

}