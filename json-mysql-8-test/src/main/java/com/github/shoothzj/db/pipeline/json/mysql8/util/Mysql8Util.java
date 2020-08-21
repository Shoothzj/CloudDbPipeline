package com.github.shoothzj.db.pipeline.json.mysql8.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.shoothzj.db.pipeline.json.mysql8.exception.NotImplementException;
import com.github.shoothzj.db.pipeline.json.mysql8.exception.NotSupportException;
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
                    c.prepareStatement("INSERT INTO " + tableName + " ("
                            + keyStr + ") VALUES (" + questionStr + ")");
            for (int i = 0; i < keys.length; i++) {
                setParam(i + 1, values[i], fieldMap.get(keys[i]), preparedStatement);
            }
            preparedStatement.execute();
        }
    }

    public static void setParam(int index, JsonNode node, FieldDescribe fieldDescribe, PreparedStatement statement) throws Exception {
        String columnTypeName = fieldDescribe.getColumnTypeName();
        if (node instanceof BooleanNode) {
            {
                BooleanNode booleanNode = (BooleanNode) node;
                if (columnTypeName.equals("BIT") || columnTypeName.equals("TINYINT")
                        || columnTypeName.equals("INT")) {
                    statement.setBoolean(index, booleanNode.booleanValue());
                    return;
                }
                throw new NotImplementException(String.format("sql %s type Not Implemented yet.", columnTypeName));
            }
        }
        if (node instanceof IntNode) {
            {
                IntNode intNode = (IntNode) node;
                if (ColumnTypeUtil.isNumberType(columnTypeName)) {
                    statement.setInt(index, intNode.intValue());
                    return;
                }
                throw new NotImplementException(String.format("sql %s type Not Implemented yet.", columnTypeName));
            }
        }
        if (node instanceof LongNode) {
            LongNode longNode = (LongNode) node;
            if (ColumnTypeUtil.isNumberType(columnTypeName)) {
                statement.setLong(index, longNode.longValue());
                return;
            }
            throw new NotImplementException(String.format("sql %s type Not Implemented yet.", columnTypeName));
        }
        if (node instanceof FloatNode) {
            if (ColumnTypeUtil.isNumberType(columnTypeName)) {
                throw new NotSupportException("Double Node can't convert to BIT");
            }
            FloatNode floatNode = (FloatNode) node;
        }
        if (node instanceof DoubleNode) {
            if (ColumnTypeUtil.isNumberType(columnTypeName)) {
                throw new NotSupportException("Double Node can't convert to BIT");
            }
            DoubleNode doubleNode = (DoubleNode) node;
        }
        if (node instanceof ArrayNode) {
            if (ColumnTypeUtil.isNumberType(columnTypeName)) {
                throw new NotSupportException("Double Node can't convert to BIT");
            }
            ArrayNode arrayNode = (ArrayNode) node;
        }
        if (node instanceof ObjectNode) {
            if (ColumnTypeUtil.isNumberType(columnTypeName)) {
                throw new NotSupportException("Double Node can't convert to BIT");
            }
            ObjectNode objectNode = (ObjectNode) node;
        }
        throw new IllegalArgumentException(String.format("jackson type %s Not implemented yet.", node.getClass()));
    }

}
