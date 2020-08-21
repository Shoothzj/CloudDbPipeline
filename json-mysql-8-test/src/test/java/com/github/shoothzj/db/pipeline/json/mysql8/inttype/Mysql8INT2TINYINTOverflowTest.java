package com.github.shoothzj.db.pipeline.json.mysql8.inttype;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.github.shoothzj.db.pipeline.json.mysql8.util.Mysql8Util;
import com.mysql.cj.jdbc.exceptions.MysqlDataTruncation;
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
public class Mysql8INT2TINYINTOverflowTest {

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
            PreparedStatement preparedStatement = c.prepareStatement("CREATE TABLE EXAMPLE_BOOLEAN_BOOLEAN_NULLABLE(field TINYINT)");
            preparedStatement.execute();
        }
    }

    @Test
    public void bInsertData() throws Exception {
        try {
            String[] keys = {"field"};
            JsonNode[] values = {new IntNode(128)};
            Mysql8Util.insertData("EXAMPLE_BOOLEAN_BOOLEAN_NULLABLE", keys, values);
            throw new IllegalAccessException("Can not reach here");
        } catch (MysqlDataTruncation ex) {
            String exMessage = ex.getMessage();
            if (exMessage.startsWith("Data truncation: Out of range value for")) {
                return;
            }
            throw new IllegalAccessException("Can not reach here");
        }
    }

    @Test
    public void cQueryData() throws Exception {
        // Now try to connect
        Properties p = new Properties();
        p.put("user", "hzj");
        p.put("password", "Mysql@123");
        try (Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/ttbb", p)) {
            PreparedStatement preparedStatement = c.prepareStatement("SELECT * FROM EXAMPLE_BOOLEAN_BOOLEAN_NULLABLE");
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
            PreparedStatement preparedStatement = c.prepareStatement("DROP TABLE EXAMPLE_BOOLEAN_BOOLEAN_NULLABLE");
            preparedStatement.execute();
        }
    }


}
