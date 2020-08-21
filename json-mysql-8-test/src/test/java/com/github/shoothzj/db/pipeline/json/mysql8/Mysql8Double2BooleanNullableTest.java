package com.github.shoothzj.db.pipeline.json.mysql8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.github.shoothzj.db.pipeline.json.mysql8.exception.NotSupportException;
import com.github.shoothzj.db.pipeline.json.mysql8.util.Mysql8Util;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * @author hezhangjian
 */
@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Mysql8Double2BooleanNullableTest {

    @Before
    public void initDriver() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
    }

    @Test
    public void aCreateTable() throws Exception {
        // Now try to connect
        Properties p = new Properties();
        p.put("user", "hzj");
        p.put("password", "Mysql@123");
        try (Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/ttbb", p)) {
            PreparedStatement preparedStatement = c.prepareStatement("CREATE TABLE EXAMPLE_DOUBLE_BOOLEAN_NULLABLE(field BOOLEAN)");
            preparedStatement.execute();
        }
    }

    @Test
    public void bInsertData() throws Exception {
        String[] keys = {"field"};
        JsonNode[] values = {new DoubleNode(1)};
        try {
            Mysql8Util.insertData("EXAMPLE_DOUBLE_BOOLEAN_NULLABLE", keys, values);
            throw new IllegalAccessException("Can not reach here");
        } catch (NotSupportException ex) {
            log.error("not support ", ex);
        }
    }

    @Test
    public void cQueryData() throws Exception {
        // Now try to connect
        Properties p = new Properties();
        p.put("user", "hzj");
        p.put("password", "Mysql@123");
        try (Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/ttbb", p)) {
            PreparedStatement preparedStatement = c.prepareStatement("SELECT * FROM EXAMPLE_DOUBLE_BOOLEAN_NULLABLE");
            ResultSet resultSet = preparedStatement.executeQuery();
            Assert.assertFalse(resultSet.next());
        }
    }

    @Test
    public void dDropTable() throws Exception {
        // Now try to connect
        Properties p = new Properties();
        p.put("user", "hzj");
        p.put("password", "Mysql@123");
        try (Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/ttbb", p)) {
            PreparedStatement preparedStatement = c.prepareStatement("DROP TABLE EXAMPLE_DOUBLE_BOOLEAN_NULLABLE");
            preparedStatement.execute();
        }
    }

}
