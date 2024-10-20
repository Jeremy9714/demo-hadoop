package com.example.demo.bigdata.common.util;

import java.util.UUID;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/18 16:58
 * @Version: 1.0
 */
public class CTools {

    public static String getUUID() {
        String uuid = UUID.randomUUID().toString();
        return uuid.replaceAll("-", "");
    }

    public static boolean isBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
