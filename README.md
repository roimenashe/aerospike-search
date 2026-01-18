# Aerospike Search

[![Build and Test](https://github.com/roimenashe/aerospike-search/actions/workflows/build.yml/badge.svg)](https://github.com/roimenashe/aerospike-search/actions/workflows/build.yml)

In-memory embedded full-text and vector search for [Aerospike](https://aerospike.com/), built on [Apache Lucene](https://github.com/apache/lucene), enabling fast keyword and semantic queries through a simple Java API.

## Features

- **Full-Text Search** - Index and search text fields using Lucene's powerful text analysis
- **Vector Search** - K-nearest neighbor (KNN) search with customizable similarity functions
- **Hybrid Search** - Combine full-text and vector search with configurable weights
- **Zero Infrastructure** - Runs embedded in your JVM, no external services required
- **Simple API** - Get started with just a few lines of code

## Requirements

- Java 21+
- Aerospike Server (running locally or remotely)
- Maven or Gradle

## Installation

Clone and install to your local Maven repository:

```bash
git clone https://github.com/roimenashe/aerospike-search.git
cd aerospike-search
mvn clean install
```

Then add the dependency to your project's `pom.xml`:

```xml
<dependency>
    <groupId>io.github.roimenashe</groupId>
    <artifactId>aerospike-search</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Quick Start

### Full-Text Search

Index all text bins and search by keyword:

```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    // Index all string bins in the set
    search.createFullTextIndex("namespace", "products");
    
    // Search for records containing "laptop"
    List<Record> results = search.searchText("namespace", "products", "laptop", 10);
}
```

Index specific bins only:

```java
search.createFullTextIndex("namespace", "products", "title", "description");
```

### Vector Search

#### From an existing vector bin

```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    // Index vectors stored in the "embedding" bin
    search.createVectorIndex("namespace", "products", "embedding", SimilarityFunction.COSINE);
    
    // Find 10 nearest neighbors
    float[] queryVector = new float[]{0.1f, 0.8f, 0.3f};
    List<Record> results = search.searchVector("namespace", "products", queryVector, 10, SimilarityFunction.COSINE);
}
```

#### Using a custom embedding function

Generate embeddings on-the-fly during indexing:

```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    // Create vectors using a custom embedding function
    search.createVectorIndex("namespace", "products", record -> {
        String description = record.getString("description");
        return myEmbeddingModel.embed(description);  // Your embedding logic
    }, SimilarityFunction.DOT_PRODUCT);
    
    float[] queryVector = myEmbeddingModel.embed("wireless headphones");
    List<Record> results = search.searchVector("namespace", "products", queryVector, 10, SimilarityFunction.DOT_PRODUCT);
}
```

### Hybrid Search

Combine keyword matching with semantic similarity:

```java
try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
    // Build both indexes
    search.createFullTextIndex("namespace", "products");
    search.createVectorIndex("namespace", "products", "embedding", SimilarityFunction.COSINE);
    
    // Hybrid search: 60% text relevance, 40% vector similarity
    float[] queryVector = myEmbeddingModel.embed("comfortable office chair");
    List<Record> results = search.searchHybrid(
        "namespace", "products",
        "ergonomic chair",           // text query
        queryVector,                 // vector query
        SimilarityFunction.COSINE,
        10,                          // limit
        0.6,                         // text weight
        0.4                          // vector weight
    );
}
```

## Limitations

- **In-memory only** - Indexes are stored in JVM heap memory and are not persisted to disk
- **No real-time updates** - Changes to Aerospike data require rebuilding the index to be reflected in search results
- **Single-node** - Indexes are local to the JVM instance and not distributed across nodes

For large-scale or distributed search use cases, consider using the
[Aerospike Elasticsearch Connector](https://aerospike.com/docs/connectors/elasticsearch/), which provides scalable
integration with Elasticsearch for enterprise-grade indexing and querying.