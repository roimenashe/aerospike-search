package com.aerospike.search;

import com.aerospike.index.VectorIndexer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VectorSearchService {

    public final VectorIndexer indexer;

    public VectorSearchService(VectorIndexer indexer) {
        this.indexer = indexer;
    }

    /**
     * Performs vector similarity search within the given (namespace:set) index.
     */
    public List<String> searchVector(String namespace, String set, float[] queryVector, int k) throws IOException {
        IndexSearcher indexSearcher = indexer.getIndexSearcher(namespace, set);
        if (indexSearcher == null) {
            throw new IllegalStateException("Index not built yet. Call createVectorIndex() first.");
        }

        Query query = new KnnFloatVectorQuery("vector", queryVector, k);
        TopDocs topDocs = indexSearcher.search(query, k);

        List<String> results = new ArrayList<>();
        for (ScoreDoc sd : topDocs.scoreDocs) {
            LeafReaderContext leaf = indexSearcher.getIndexReader().leaves().get(ReaderUtil.subIndex(sd.doc, indexSearcher.getIndexReader().leaves()));
            StoredFields storedFields = leaf.reader().storedFields();
            Document doc = storedFields.document(sd.doc - leaf.docBase);
            results.add(doc.get("id"));
        }

        return results;
    }
}
