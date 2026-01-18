package io.github.roimenashe.search;

import io.github.roimenashe.index.FullTextIndexer;
import io.github.roimenashe.model.ScoredId;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.util.ArrayList;
import java.util.List;

public class FullTextSearchService {

    private final FullTextIndexer indexer;

    public FullTextSearchService(FullTextIndexer indexer) {
        this.indexer = indexer;
    }

    public List<String> searchText(String namespace, String set, String queryStr, int limit) throws Exception {
        IndexSearcher indexSearcher = indexer.getIndexSearcher(namespace, set);
        if (indexSearcher == null) {
            throw new IllegalStateException("Index not built yet. Call createFullTextIndex() first.");
        }

        String[] fieldNames = extractFieldNames(indexSearcher.getIndexReader());
        Query query = new MultiFieldQueryParser(fieldNames, indexer.getAnalyzer()).parse(queryStr);
        TopDocs topDocs = indexSearcher.search(query, limit);

        List<String> results = new ArrayList<>();
        for (ScoreDoc sd : topDocs.scoreDocs) {
            Document doc = getDocument(indexSearcher, sd);
            results.add(doc.get("id"));
        }

        return results;
    }

    public List<ScoredId> searchWithScores(String namespace, String set,
                                           String queryStr, int limit) throws Exception {
        IndexSearcher indexSearcher = indexer.getIndexSearcher(namespace, set);
        if (indexSearcher == null) {
            throw new IllegalStateException("Index not built yet. Call createFullTextIndex() first.");
        }

        String[] fieldNames = extractFieldNames(indexSearcher.getIndexReader());
        Query query = new MultiFieldQueryParser(fieldNames, indexer.getAnalyzer()).parse(queryStr);
        TopDocs topDocs = indexSearcher.search(query, limit);

        List<ScoredId> results = new ArrayList<>();
        for (ScoreDoc sd : topDocs.scoreDocs) {
            Document doc = getDocument(indexSearcher, sd);
            results.add(new ScoredId(doc.get("id"), sd.score));
        }
        return results;
    }

    private String[] extractFieldNames(IndexReader reader) {
        return reader.leaves().stream()
                .flatMap(l -> {
                    List<String> fields = new ArrayList<>();
                    for (FieldInfo fi : l.reader().getFieldInfos()) {
                        fields.add(fi.name);
                    }
                    return fields.stream();
                })
                .filter(name -> !"id".equals(name))
                .distinct()
                .toArray(String[]::new);
    }

    private Document getDocument(IndexSearcher indexSearcher, ScoreDoc sd) throws Exception {
        LeafReaderContext leaf = indexSearcher.getIndexReader().leaves()
                .get(ReaderUtil.subIndex(sd.doc, indexSearcher.getIndexReader().leaves()));
        StoredFields storedFields = leaf.reader().storedFields();
        return storedFields.document(sd.doc - leaf.docBase);
    }
}
