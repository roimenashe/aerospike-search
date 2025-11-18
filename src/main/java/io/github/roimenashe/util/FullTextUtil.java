package io.github.roimenashe.util;

public class FullTextUtil {

    public static String getFullTextUniqueIndexName(String namespace, String set) {
        return namespace + ":" + set;
    }
}
