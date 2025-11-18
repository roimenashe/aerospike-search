package io.github.roimenashe.search;

import io.github.roimenashe.index.FullTextIndexer;
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

        IndexReader reader = indexSearcher.getIndexReader();
        String[] fieldNames = reader.leaves().stream()
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

        Query query = new MultiFieldQueryParser(fieldNames, indexer.getAnalyzer()).parse(queryStr);
        TopDocs topDocs = indexSearcher.search(query, limit);

        List<String> results = new ArrayList<>();
        for (ScoreDoc sd : topDocs.scoreDocs) {
            LeafReaderContext leaf = indexSearcher.getIndexReader().leaves().get(ReaderUtil.subIndex(sd.doc, indexSearcher.getIndexReader().leaves()));
            StoredFields storedFields = leaf.reader().storedFields();
            Document doc = storedFields.document(sd.doc - leaf.docBase);
            results.add(doc.get("id"));
        }

        return results;
    }

    public List<HybridSearchService.ScoredId> searchWithScores(String namespace, String set,
                                                               String queryStr, int limit) throws Exception {
        IndexSearcher indexSearcher = indexer.getIndexSearcher(namespace, set);
        if (indexSearcher == null)
            throw new IllegalStateException("Index not built yet.");

        FieldInfos fieldInfos = indexSearcher.getIndexReader()
                .leaves()
                .getFirst()
                .reader()
                .getFieldInfos();

        List<String> fields = new ArrayList<>();
        for (FieldInfo fi : fieldInfos) {
            if (!"id".equals(fi.name)) {
                fields.add(fi.name);
            }
        }
        String[] fieldNames = fields.toArray(new String[0]);

        Query query = new MultiFieldQueryParser(fieldNames, indexer.getAnalyzer()).parse(queryStr);
        TopDocs topDocs = indexSearcher.search(query, limit);

        List<HybridSearchService.ScoredId> results = new ArrayList<>();
        for (ScoreDoc sd : topDocs.scoreDocs) {
            LeafReaderContext leaf = indexSearcher.getIndexReader().leaves().get(ReaderUtil.subIndex(sd.doc, indexSearcher.getIndexReader().leaves()));
            StoredFields storedFields = leaf.reader().storedFields();
            Document doc = storedFields.document(sd.doc - leaf.docBase);
            results.add(new HybridSearchService.ScoredId(doc.get("id"), sd.score));
        }
        return results;
    }
}
