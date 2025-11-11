# Aerospike Search
[![Build and Test](https://github.com/roimenashe/aerospike-search/actions/workflows/build.yml/badge.svg)](https://github.com/roimenashe/aerospike-search/actions/workflows/build.yml)

An in-memory full-text search layer for Aerospike, powered by Apache Lucene.
It scans Aerospike records, builds per-namespace + set Lucene indexes, and enables fast text queries, including fuzzy, wildcard, and field-specific searches.

Basic Example:
```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    search.createFullTextIndex("namespace1", "usersSet");
    List<Record> results = search.searchText("namespace1", "usersSet", "Lucene~1", 10);
    results.forEach(System.out::println);
}
```