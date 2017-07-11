package cn.tanyf.elasticsearch.synonym;

import cn.tanyf.elasticsearch.BaseTestCase;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.testng.annotations.Test;

import java.io.IOException;

public class SynonymTest extends BaseTestCase{
	private String indexType = "test_synonym";
    private String indexName = "test_synonym";
    @Test
    public void testIndex() {
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(indexName).execute()
                .actionGet();
        if (indicesExistsResponse.isExists()) {
            client.admin().indices().prepareDelete(indexName).execute().actionGet().isAcknowledged();
        }
        client.admin().indices().prepareCreate(indexName).addMapping(indexType, createMapping()).execute()
                .actionGet();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(createSource("西红柿蛋汤")));
        bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(createSource("番茄炒鸡蛋")));
        bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(createSource("马铃薯可口美味")));
        bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(createSource("土豆好吃吗")));
        bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(createSource("test bb")));
        bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(createSource("text aa")));
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            System.out.println("create index fail:" + bulkResponse.buildFailureMessage());
        }
    }
 
    @Test
    public void testSearch(){
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setTypes(indexType).setQuery(QueryBuilders.queryStringQuery("番茄").field("name").analyzer("ik_max_word_syno"))
                .setFrom(0).setSize(20).setExplain(true);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        for (SearchHit searchHit : searchResponse.getHits()) {
            System.out.println(searchHit.getSource().get("name"));
        }
    }
 
    public XContentBuilder createMapping() {
        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(indexType);
            mapping.startObject("properties");
            mapping.startObject("name").field("type", "text").field("store", "yes")
                        .field("analyzer", "index_ansj").field("search_analyzer", "query_ansj")
                    .endObject();
            mapping.endObject().endObject().endObject();
            System.out.println(mapping.string());
            return mapping;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public XContentBuilder createSource(String name) {
        try {
            XContentBuilder data = XContentFactory.jsonBuilder().startObject();
            data.field("name", name);
            data.endObject();
            return data;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
