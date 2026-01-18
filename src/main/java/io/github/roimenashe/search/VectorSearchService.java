package io.github.roimenashe.search;

import io.github.roimenashe.index.VectorIndexer;
import io.github.roimenashe.model.ScoredId;
import io.github.roimenashe.model.SimilarityFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VectorSearchService {

    private final VectorIndexer indexer;

    public VectorSearchService(VectorIndexer indexer) {
        this.indexer = indexer;
    }

    public List<String> searchVector(String namespace, String set, float[] queryVector, int k,
                                     SimilarityFunction similarityFunction) throws IOException {
        IndexSearcher indexSearcher = indexer.getIndexSearcher(namespace, set, similarityFunction);
        if (indexSearcher == null) {
            throw new IllegalStateException("Vector index not built for similarityFunction: " + similarityFunction);
        }

        Query query = new KnnFloatVectorQuery("vector", queryVector, k);
        TopDocs topDocs = indexSearcher.search(query, k);

        List<String> results = new ArrayList<>();
        for (ScoreDoc sd : topDocs.scoreDocs) {
            Document doc = getDocument(indexSearcher, sd);
            results.add(doc.get("id"));
        }

        return results;
    }

    public List<ScoredId> searchWithScores(String namespace, String set, float[] queryVector, int k,
                                           SimilarityFunction similarityFunction) throws IOException {
        IndexSearcher indexSearcher = indexer.getIndexSearcher(namespace, set, similarityFunction);
        if (indexSearcher == null) {
            throw new IllegalStateException("Vector index not built for similarityFunction: " + similarityFunction);
        }

        Query query = new KnnFloatVectorQuery("vector", queryVector, k);
        TopDocs topDocs = indexSearcher.search(query, k);

        List<ScoredId> results = new ArrayList<>();
        for (ScoreDoc sd : topDocs.scoreDocs) {
            Document doc = getDocument(indexSearcher, sd);
            results.add(new ScoredId(doc.get("id"), sd.score));
        }
        return results;
    }

    private Document getDocument(IndexSearcher indexSearcher, ScoreDoc sd) throws IOException {
        LeafReaderContext leaf = indexSearcher.getIndexReader().leaves()
                .get(ReaderUtil.subIndex(sd.doc, indexSearcher.getIndexReader().leaves()));
        StoredFields storedFields = leaf.reader().storedFields();
        return storedFields.document(sd.doc - leaf.docBase);
    }
}
