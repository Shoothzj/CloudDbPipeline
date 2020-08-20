package com.github.shoothzj.db.pipeline.json.mysql8.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.github.shoothzj.db.pipeline.json.mysql8.module.FieldDescribe;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author hezhangjian
 */
@Slf4j
public class Mysql8Util {

    public static void insertData(String tableName, String[] keys, JsonNode[] values) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        // Now try to connect
        Properties p = new Properties();
        p.put("user", "hzj");
        p.put("password", "Mysql@123");

        try (Connection c = DriverManager.getConnection("jdbc:mysql://localhost:3306/ttbb", p)) {
            Map<String, FieldDescribe> fieldMap = new HashMap<>();
            {
                PreparedStatement statement = c.prepareStatement("SELECT * FROM " + tableName + " LIMIT 1");
                ResultSetMetaData metaData = statement.getMetaData();
                int size = metaData.getColumnCount();
                for (int i = 1; i <= size; i++) {
                    FieldDescribe fieldDescribe = new FieldDescribe();
                    fieldDescribe.setColumnType(metaData.getColumnType(i));
                    fieldDescribe.setColumnTypeName(metaData.getColumnTypeName(i));
                    fieldDescribe.setColumnDisplaySize(metaData.getColumnDisplaySize(i));
                    fieldDescribe.setColumnLabel(metaData.getColumnLabel(i));
                    fieldMap.put(metaData.getColumnName(i), fieldDescribe);
                    log.info("[{}]", fieldDescribe);
                }
            }
            String keyStr = Arrays.stream(keys).collect(Collectors.joining(","));
            String[] questionArray = new String[keys.length];
            Arrays.fill(questionArray, "?");
            String questionStr = Arrays.stream(questionArray).collect(Collectors.joining(","));
            PreparedStatement preparedStatement =
                    c.prepareStatement("INSERT INTO EXAMPLE_NULL_BOOLEAN_NULLABLE ("
                            + keyStr + ") VALUES ("  + questionStr + ")");
            for (int i = 0; i < keys.length; i++) {
                JsonNode node = values[i];
                if (node instanceof IntNode) {
                    {
                        IntNode intNode = (IntNode) node;
                        String columnTypeName = fieldMap.get(keys[i]).getColumnTypeName();
                        if (columnTypeName.equals("BIT")) {
                            preparedStatement.setBoolean(i + 1, intNode.intValue() != 0);
                            continue;
                        }
                        throw new IllegalArgumentException(String.format("sql %s type Not Implemented yet.", columnTypeName));
                    }
                }
                if (node instanceof LongNode) {
                    LongNode longNode = (LongNode) node;
                    String columnTypeName = fieldMap.get(keys[i]).getColumnTypeName();
                    if (columnTypeName.equals("BIT")) {
                        preparedStatement.setBoolean(i + 1, longNode.longValue() != 0);
                        continue;
                    }
                    throw new IllegalArgumentException(String.format("sql %s type Not Implemented yet.", columnTypeName));
                }
                throw new IllegalArgumentException(String.format("jackson type %s Not implemented yet.", values[i].getClass()));
            }
            preparedStatement.execute();
        }
    }

}
