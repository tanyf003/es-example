package cn.tanyf.elasticsearch.query;


import cn.tanyf.elasticsearch.BaseTestCase;
import cn.tanyf.elasticsearch.domain.FlAsk;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.putMappingRequest;

/**
 * 
 * 查询索引
 * @author hg_tyf@163.com
 */
public class QueryIndexTest extends BaseTestCase{
    private String ASK_INDEX_NAME="fl_ask";
    private String ASK_TYPE_NAME="ask";
    private String ASK_ID="1";
    private boolean  EXPLAIN=false; // 类似 sql explain
    @Test
    public void queryString() throws Exception {
        // 包含kimchy 不包含elasticsearch
        // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
//        QueryBuilder qb = QueryBuilders.queryString("+kimchy -elasticsearch").field("content").field("title");
        // 通配符 匹配字段 例如content，表达式cont* 可以匹配 而contd*不匹配
        QueryBuilder qb = QueryBuilders.queryStringQuery("+kimchy -elasticsearch").field("cont*");
        System.out.println(qb.toString());
        //            QueryBuilder qb = QueryBuilders.queryString("java");
        SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes(TYPE_NAME).setQuery(qb).setSize(10)
                .setFrom(0).setExplain(EXPLAIN).execute().actionGet();
        String responseStr = response.toString();
        System.out.println(responseStr);
        SearchHit[] hits = response.getHits().getHits();
        if (null != hits && hits.length > 0) {
            for (SearchHit hit : hits) {
                // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                System.out.println(hit.getSourceAsString());
                Map<String, Object> fields = hit.getSource();
                System.out.println(fields);
                if (fields.containsKey("title")) {
                    System.out.println("title:" + fields.get("title"));
                }
                if (fields.containsKey("content")) {
                    System.out.println("content:" + fields.get("content"));
                }
            }
        }
    }
    @Test
    public void prefixQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.prefixQuery("title", "贵州");
        System.out.println(qb.toString());
        //            QueryBuilder qb = QueryBuilders.queryString("java");
        SearchResponse response = client.prepareSearch(ASK_INDEX_NAME).setTypes(ASK_TYPE_NAME).setQuery(qb).setSize(10)
                .setFrom(0).setExplain(EXPLAIN).execute().actionGet();
        String responseStr = response.toString();
        System.out.println(responseStr);
        SearchHit[] hits = response.getHits().getHits();
        if (null != hits && hits.length > 0) {
            for (SearchHit hit : hits) {
                // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                System.out.println(hit.getSourceAsString());
                Map<String, Object> fields = hit.getSource();
                System.out.println(fields);
                if (fields.containsKey("title")) {
                    System.out.println("title:" + fields.get("title"));
                }
                if (fields.containsKey("content")) {
                    System.out.println("content:" + fields.get("content"));
                }
            }
        }
    }
    @Test
    public void termsQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.termsQuery("content", "因为");
        //            QueryBuilder qb = QueryBuilders.queryString("java");
        SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes(TYPE_NAME).setQuery(qb).setSize(10)
                .setFrom(0).setExplain(true).execute().actionGet();
        String responseStr = response.toString();
        System.out.println(responseStr);
        SearchHit[] hits = response.getHits().getHits();
        if (null != hits && hits.length > 0) {
            for (SearchHit hit : hits) {
                // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                System.out.println(hit.getSourceAsString());
                Map<String, Object> fields = hit.getSource();
                System.out.println(fields);
                if (fields.containsKey("title")) {
                    System.out.println("title:" + fields.get("title"));
                }
                if (fields.containsKey("content")) {
                    System.out.println("content:" + fields.get("content"));
                }
            }
        }
    }
    @Test
    public void boolQuery() throws Exception {
//        QueryBuilder qb = QueryBuilders.boolQuery()
//                                .must(QueryBuilders.termQuery("content", "因为")) // must 类似 sql语句中的 = 集合取AND
//                                .must(QueryBuilders.termQuery("title", "因为")); 
//        QueryBuilder qb = QueryBuilders.boolQuery()
//                                .must(QueryBuilders.termQuery("content", "因为")) // must 类似 sql语句中的 =
//                                .mustNot(QueryBuilders.termQuery("title", "因为")); // mustNot 类似 sql语句中的 ！= 集合取NOT
        QueryBuilder qb = QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("content", "工资")) // must 类似 sql语句中的 =
                                //.mustNot(QueryBuilders.termQuery("title", "因为"))// mustNot 类似 sql语句中的 ！=
                                .should(QueryBuilders.termQuery("title", "工资")); //集合取OR or should http://kernaling-wong.iteye.com/blog/456859
        //          QueryBuilder qb = QueryBuilders.queryString("java");
        System.out.println(qb.toString());
        SearchResponse response = client.prepareSearch(ASK_INDEX_NAME).setTypes(ASK_TYPE_NAME).setQuery(qb).setSize(3)
                .setFrom(0)/*.addSort("id", SortOrder.ASC)*/.setExplain(EXPLAIN).execute().actionGet();
        System.out.println(response.toString());
        SearchHit[] hits = response.getHits().getHits();
        List<FlAsk> asks = null;
         if (null != hits && hits.length > 0) {
             asks = new ArrayList<FlAsk>();
            for (SearchHit hit : hits) {
                // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                System.out.println(hit.getSourceAsString());
               FlAsk ask=  JSON.parseObject(hit.getSourceAsString(), FlAsk.class);
               asks.add(ask);
            }
        }
         System.out.println(asks);
    }
    
    @Test
    public void Scroll() throws Exception {
        QueryBuilder qb = QueryBuilders.termQuery("content", "工资");
        
        SearchResponse response = client.prepareSearch(ASK_INDEX_NAME).setTypes(ASK_TYPE_NAME)
                .setScroll(new TimeValue(1000))
                .setQuery(qb)
                .setSize(3)
                .setFrom(0)/*.addSort("id", SortOrder.ASC)*/.setExplain(EXPLAIN).execute().actionGet();
        List<FlAsk> asks = new ArrayList<FlAsk>();
        while (true) {
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(1000)).execute().actionGet();
            boolean hitsRead = false;
            for (SearchHit hit : response.getHits()) {
                hitsRead = true;
//                System.out.println(hit.getSourceAsString());
                FlAsk ask=  JSON.parseObject(hit.getSourceAsString(), FlAsk.class);
                asks.add(ask);
            }
            //Break condition: No hits are returned
            if (!hitsRead) {
                break;
            }
        }
        System.out.println(asks);
    }

    @Test
    public void putMapping() throws IOException{
        AdminClient adminClient = client.admin();
        IndicesAdminClient adminClientIndices = adminClient.indices();
        IndicesExistsRequest re = new IndicesExistsRequestBuilder(adminClientIndices,IndicesExistsAction.INSTANCE,INDEX_NAME).request();
        boolean flag = adminClientIndices.exists(re).actionGet().isExists();
        if(flag){
            adminClientIndices.delete(new DeleteIndexRequest(INDEX_NAME));
        }
        // 预定义一个索引
        adminClientIndices.prepareCreate(INDEX_NAME).execute().actionGet();
        // 定义索引字段属性
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
        mapping = mapping
                .startObject(INDEX_NAME)//{index:{_id:{}}
                    .startObject("_id").field("path", "id").endObject()  // _id:{path:id}
                    //.field("_type",TYPE_NAME) // _type:typeName
                    .startObject("properties")
                            .startObject("id") 
                            .field("type", "long") 
                            .endObject()
                            .startObject("title")  
                                .field("type", "string")             
                                .field("analyzer", "index_ansj")
                                .field("search_analyzer", "query_ansj")
                             .endObject()
                             .startObject("content")  
                             .field("type", "string")
                            .field("analyzer", "index_ansj")
                            .field("search_analyzer", "query_ansj")
                            .endObject()
                   .endObject()  
                .endObject(); 
        mapping = mapping.endObject();
        // {"goods":{"_id":{"path":"id"},"properties":{"id":{"type":"long"},"title":{"type":"string","indexAnalyzer":"mmseg","searchAnalyzer":"mmseg"},"content":{"type":"string","indexAnalyzer":"mmseg","searchAnalyzer":"mmseg"}}}}
        System.out.println(mapping.string());
        PutMappingRequest mappingRequest = Requests.putMappingRequest(INDEX_NAME).type(TYPE_NAME).source(mapping);
        adminClientIndices.putMapping(mappingRequest).actionGet();
    }
   
    String TEST_INDEX_NAME="test";
    String TEST_DOC_TYPE_NAME="test";
    @Test
    public void putMappingJson() throws IOException{
        String mapping = Streams.copyToString(new FileReader("/mapping/test-mapping.json"));
        AdminClient adminClient = client.admin();
        IndicesAdminClient adminClientIndices = adminClient.indices();
        IndicesExistsRequest re = new IndicesExistsRequestBuilder(adminClientIndices,IndicesExistsAction.INSTANCE,"test").request();
        boolean flag = adminClientIndices.exists(re).actionGet().isExists();
        if(flag){
            adminClientIndices.delete(new DeleteIndexRequest("test"));
        }
        // 预定义一个索引
        adminClientIndices.prepareCreate("test").execute().actionGet();
        client.admin().indices().putMapping(putMappingRequest("test").type("test")
        															.source( XContentFactory.xContentType(mapping)))
        															.actionGet();
    }
}
