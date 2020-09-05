package com.github.shoothzj.db.pipeline.test.integrate;

import com.github.shoothzj.db.pipeline.plugin.mysql8.util.MysqlConcatUtil;
import com.github.shoothzj.javatool.util.LogUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlDataRewindTest {

    public void firstQueryTest() {
        final String firstQuerySql = MysqlConcatUtil.firstQuerySql("user", "name", 10);
        System.out.println(firstQuerySql);
    }

    public void queryTest() {
        final String subQuerySql = MysqlConcatUtil.subQuerySql("user", "name", 10, 10);
        System.out.println(subQuerySql);
    }

    public static void main(String[] args) {
        LogUtil.configureLog();
        DataRewind dataRewind = new DataRewind();
        dataRewind.startRewindTaskWithRelativePath("rewind/mysql_sample.yaml");
    }


}
