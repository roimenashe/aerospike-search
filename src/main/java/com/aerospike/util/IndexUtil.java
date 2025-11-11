package com.aerospike.util;

public class IndexUtil {

    public static String getUniqueIndexName(String namespace, String set) {
        return namespace + ":" + set;
    }
}
