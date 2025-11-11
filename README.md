# Aerospike Search
[![Build and Test](https://github.com/roimenashe/aerospike-search/actions/workflows/build.yml/badge.svg)](https://github.com/roimenashe/aerospike-search/actions/workflows/build.yml)

In-memory embedded full-text and vector search for Aerospike, built on Apache Lucene, enabling fast keyword and semantic queries through a simple Java API.

### Basic Examples

#### Full-Text
```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    search.createFullTextIndex("namespace1", "set1");
    List<Record> results = search.searchText("namespace1", "set1", "Lucene", 10);
}
```

#### Vector
```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    search.createVectorIndex("namespace1", "set1", record -> {
        String text = record.getString("bio");
        float lucene = text != null && text.contains("Lucene") ? 1f : 0f;
        float aerospike = text != null && text.contains("Aerospike") ? 1f : 0f;
        float searchWord = text != null && text.contains("search") ? 1f : 0f;
        return new float[]{lucene, aerospike, searchWord};
    });
    float[] queryVector = new float[]{1f, 0f, 1f};
    List<Record> results = search.searchVector("namespace1", "set1", queryVector, 10);
}
```
