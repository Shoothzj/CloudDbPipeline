package com.github.shoothzj.db.pipeline.plugin.mysql8.util;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hezhangjian
 */
@Slf4j
public class MysqlConcatUtil {

    public static String firstQuerySql(String tableName, String primaryKey, long pageSize) {
        // 子查询分页 SELECT * FROM $TABLE_NAME
        // WHERE $PRIMARY_KEY >= (SELECT $PRIMARY_KEY FROM $TABLE_NAME ORDER BY $PRIMARY_KEY asc LIMIT ${(page-1)*pageSize} )
        // ORDER BY $PRIMARY_KEY asc LIMIT $PAGE_SIZE
        String phase1Sql = "SELECT * FROM " + tableName;
        String phase2Sql = " WHERE " + primaryKey + " >= (SELECT " + primaryKey + " FROM " + tableName;
        String phase3Sql = " ORDER BY " + primaryKey + " asc LIMIT 1 )";
        String phase4Sql = " ORDER BY " + primaryKey + " asc LIMIT " + pageSize;
        return phase1Sql + phase2Sql + phase3Sql + phase4Sql + ";";
    }

    public static String subQuerySql(String tableName, String primaryKey, long skip, long pageSize) {
        // 子查询分页 SELECT * FROM $TABLE_NAME
        // WHERE $PRIMARY_KEY >= (SELECT $PRIMARY_KEY FROM $TABLE_NAME ORDER BY $PRIMARY_KEY asc LIMIT ${(page-1)*pageSize} )
        // ORDER BY $PRIMARY_KEY asc LIMIT $PAGE_SIZE
        String phase1Sql = "SELECT * FROM " + tableName;
        String phase2Sql = " WHERE " + primaryKey + " >= (SELECT " + primaryKey + " FROM " + tableName;
        String phase3Sql = " ORDER BY " + primaryKey + " asc LIMIT " + skip + ",1 )";
        String phase4Sql = " ORDER BY " + primaryKey + " asc LIMIT " + pageSize;
        return phase1Sql + phase2Sql + phase3Sql + phase4Sql + ";";
    }

    // UPDATE $TABLE_NAME SET $field = ? WHERE $PRIMARY_KEY = ?;
    public static String getUpdateSql(String tableName, List<String> field, String primaryKey) {
        String phase1Sql = "UPDATE " + tableName + " SET ";
        String phase2Sql = field.stream().map(str -> str + " = ?").collect(Collectors.joining(","));
        String phase3Sql = " WHERE " + primaryKey + " = ?";
        return phase1Sql + phase2Sql + phase3Sql;
    }

}
