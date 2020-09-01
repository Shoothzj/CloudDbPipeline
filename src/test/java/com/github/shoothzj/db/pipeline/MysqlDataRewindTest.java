package com.github.shoothzj.db.pipeline;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlDataRewindTest {

    @Test
    public void firstQueryTest() {
        final String firstQuerySql = MysqlDataRewind.firstQuerySql("user", "name", 10);
        System.out.println(firstQuerySql);
    }

    @Test
    public void queryTest() {
        final String subQuerySql = MysqlDataRewind.subQuerySql("user", "name", 10, 10);
        System.out.println(subQuerySql);
    }

}
