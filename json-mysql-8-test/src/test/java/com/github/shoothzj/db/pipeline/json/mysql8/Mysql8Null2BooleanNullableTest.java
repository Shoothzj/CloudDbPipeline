package com.github.shoothzj.db.pipeline.json.mysql8;

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
public class Mysql8Null2BooleanNullableTest {

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
            PreparedStatement preparedStatement = c.prepareStatement("CREATE TABLE EXAMPLE_NULL_BOOLEAN_NULLABLE(id VARCHAR(100) PRIMARY KEY, field BOOLEAN)");
            preparedStatement.execute();
        }
    }

    @Test
    public void bInsertData() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        // Now try to connect
        Properties p = new Properties();
        p.put("user", "hzj");
        p.put("password", "Mysql@123");
        try (Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/ttbb", p)) {
            PreparedStatement preparedStatement = c.prepareStatement("INSERT INTO EXAMPLE_NULL_BOOLEAN_NULLABLE (id) VALUES (?)");
            preparedStatement.setString(1, "1");
            preparedStatement.execute();
        }
    }

    @Test
    public void cQueryData() throws Exception {
        // Now try to connect
        Properties p = new Properties();
        p.put("user", "hzj");
        p.put("password", "Mysql@123");
        try (Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/ttbb", p)) {
            PreparedStatement preparedStatement = c.prepareStatement("SELECT * FROM EXAMPLE_NULL_BOOLEAN_NULLABLE");
            ResultSet resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            log.info("id is [{}], field is [{}]", resultSet.getString("id"), resultSet.getBoolean("field"));
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
            PreparedStatement preparedStatement = c.prepareStatement("DROP TABLE EXAMPLE_NULL_BOOLEAN_NULLABLE");
            preparedStatement.execute();
        }
    }


}
