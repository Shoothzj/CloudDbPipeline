package com.github.shoothzj.db.pipeline.json.mysql8.util;

import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public class ColumnTypeUtil {

    public static boolean isNumberType(String columnTypeName) {
        return columnTypeName.equals("BIT") || columnTypeName.equals("TINYINT")
                || columnTypeName.equals("INT");
    }

}
