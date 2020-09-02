package com.github.shoothzj.db.pipeline;

import com.github.shoothzj.db.pipeline.util.MysqlConcatUtil;
import com.github.shoothzj.javatool.util.LogUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlDataRewindTest {

    @Test
    public void firstQueryTest() {
        final String firstQuerySql = MysqlConcatUtil.firstQuerySql("user", "name", 10);
        System.out.println(firstQuerySql);
    }

    @Test
    public void queryTest() {
        final String subQuerySql = MysqlConcatUtil.subQuerySql("user", "name", 10, 10);
        System.out.println(subQuerySql);
    }

    @Test
    public void main() {
        LogUtil.configureLog();
        DataRewind dataRewind = new DataRewind();
        dataRewind.startRewindTaskWithAbsolutePath("rewind/mysql_sample.yaml");
    }


}
