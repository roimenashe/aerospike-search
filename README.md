# Aerospike Search
[![Build and Test](https://github.com/roimenashe/aerospike-search/actions/workflows/build.yml/badge.svg)](https://github.com/roimenashe/aerospike-search/actions/workflows/build.yml)

In-memory embedded full-text and vector search for [Aerospike](https://aerospike.com/), built on [Apache Lucene](https://github.com/apache/lucene), enabling fast keyword and semantic queries through a simple Java API.

## Basic Examples

### Full-Text
```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    search.createFullTextIndex("namespace1", "set1");
    List<Record> results = search.searchText("namespace1", "set1", "Lucene", 10);
}
```

### Vector

#### Using pre-defined Vector Bin in Aerospike
```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    search.createVectorIndex("namespace1", "set1", "vectorBin", SimilarityFunction.DOT_PRODUCT);
    float[] queryVector = new float[]{1f, 0f, 1f};
    List<Record> results = search.searchVector("namespace1", "set1", queryVector, 10, SimilarityFunction.DOT_PRODUCT);
}
```

#### Using provided embedding function
```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    search.createVectorIndex("namespace1", "set1", record -> {
        String text = record.getString("bio");
        float lucene = text != null && text.contains("Lucene") ? 1f : 0f;
        float aerospike = text != null && text.contains("Aerospike") ? 1f : 0f;
        float searchWord = text != null && text.contains("search") ? 1f : 0f;
        return new float[]{lucene, aerospike, searchWord};
    }, SimilarityFunction.DOT_PRODUCT);
    float[] queryVector = new float[]{1f, 0f, 1f};
    List<Record> results = search.searchVector("namespace1", "set1", queryVector, 10, SimilarityFunction.DOT_PRODUCT);
}
```

### Hybrid (Full-Text + Vector)
```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    search.createFullTextIndex("namespace1", "set1");
    search.createVectorIndex("namespace1", "set1", "vectorBin", SimilarityFunction.DOT_PRODUCT);

    float[] queryVector = new float[]{1f, 0f, 1f};
    List<Record> results = search.searchHybrid("namespace1", "set1", "Lucene", queryVector, 10, 0.6, 0.4, SimilarityFunction.DOT_PRODUCT);
}
```


## Notes
* For large-scale or distributed search use cases, consider using the
[Aerospike Elasticsearch Connector](https://aerospike.com/docs/connectors/elasticsearch/), which provides scalable
integration with Elasticsearch for enterprise-grade indexing and querying.