package cn.tanyf.elasticsearch.index;


import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.Test;

import cn.tanyf.elasticsearch.BaseTestCase;

import com.alibaba.fastjson.JSON;

/**
 * 
 * 创建索引
 */
public class CreateIndexTest extends BaseTestCase{
    private String INDEX_NAME="test_id";
    private String TYPE_NAME="test_id";
    private String ID="1";
    @Test
    public void createIndex() throws Exception{
       IndexResponse indexResponse = client
               .prepareIndex(INDEX_NAME, TYPE_NAME,"1")
               //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
               .setSource(XContentFactory.jsonBuilder().startObject()
                               .field("id",1)
                               .field("title", "宁伤痕为2")
                               .field("name", "kimchy elasticsearch 因为带有以宁伤痕为代价的美丽风景总是让人不由地惴惴不安，在myboy紧接着袭面而来的抑或是病痛抑或是灾难，没有谁会能够安逸着恬然，myboy因为模糊让人撕心裂肺地想呐喊。")
                               .endObject()).execute().actionGet();
       IndexResponse indexResponse2 = client
               .prepareIndex(INDEX_NAME, TYPE_NAME,"2")
               //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
               .setSource(XContentFactory.jsonBuilder().startObject()
                               .field("id",2)
                               .field("title", "宁伤痕为2")
                               .field("name", "kimchy  因为带有以宁伤痕为代价的美丽风景总是让人不由地惴惴不安，在myboy紧接着袭面而来的抑或是病痛抑或是灾难，没有谁会能够安逸着恬然，myboy因为模糊让人撕心裂肺地想呐喊。")
                               .endObject()).execute().actionGet();
       System.out.println(indexResponse.getId());
    }
    
    
    @Test
    public void queryString() throws Exception {
        // 包含kimchy 不包含elasticsearch
        // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
//        QueryBuilder qb = QueryBuilders.queryString("+kimchy -elasticsearch").field("content").field("title");
        // 通配符 匹配字段 例如content，表达式cont* 可以匹配 而contd*不匹配
        QueryBuilder qb = QueryBuilders.queryStringQuery(/*"+kimchy -elasticsearch"*/ "宁伤痕为").analyzer("ik_smart");
        SearchRequestBuilder addHighlightedField = client.prepareSearch(INDEX_NAME).setTypes(TYPE_NAME).setQuery(qb)
        		.setSize(10)
                .setFrom(0).setExplain(false)
                .highlighter(new HighlightBuilder().field("title",-1,0));
        System.out.println(addHighlightedField.toString());
		SearchResponse response = addHighlightedField.execute().actionGet();
        String responseStr = response.toString();
        System.out.println(responseStr);
        SearchHit[] hits = response.getHits().getHits();
        if (null != hits && hits.length > 0) {
            for (SearchHit hit : hits) {
                // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                //System.out.println(hit.getSourceAsString());
                Map<String, Object> fields = hit.getSource();
                System.out.println(fields);
                if (fields.containsKey("title")) {
                    System.out.println("title:" + fields.get("title"));
                }
                if (fields.containsKey("name")) {
                    System.out.println("name:" + fields.get("name"));
                }
                System.out.println(hit.highlightFields());
                String str =
                        hit.highlightFields().get("title").fragments()[0].string();
//                       String str = hit.getSource().get("title").toString();
                       System.out.println(str);
            }
        }
    }
    
    @Test
    public void getIndex() throws IOException
    { 
    	AdminClient adminClient = client.admin();
    	IndicesAdminClient adminClientIndices = adminClient.indices();
    	GetMappingsResponse gmr = new GetMappingsRequestBuilder(adminClientIndices,GetMappingsAction.INSTANCE).get();
    	// 所有索引
    	System.out.println(gmr.mappings().keys());
    	IndicesExistsRequest re = new IndicesExistsRequestBuilder(adminClientIndices,IndicesExistsAction.INSTANCE,INDEX_NAME).request();
    	 boolean flag = adminClientIndices.exists(re).actionGet().isExists();
    	 if(flag){
    		 GetMappingsResponse  res = new GetMappingsRequestBuilder(adminClientIndices,GetMappingsAction.INSTANCE, INDEX_NAME).get();
    		 MappingMetaData mmd = res.mappings().get(INDEX_NAME).get(TYPE_NAME);
    		 System.out.println(res.mappings().keys());// 索引
    		 System.out.println(mmd.getSourceAsMap().get("properties"));
    		 System.out.println(JSON.toJSONString(mmd));
    	 }
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
                    //.startObject("_id").field("path", "id").endObject()  // _id:{path:id}
                    .startObject("_all")
                    	.field("indexAnalyzer", "ik")
                    	.field("searchAnalyzer","ik")
                    	.field("term_vector","with_positions_offsets")
                    	.field("store",true)
                    	.field("enabled",true)
                    .endObject() 
                    // _id:{path:id}
                    //.field("_type",TYPE_NAME) // _type:typeName
                    .startObject("properties")
                            .startObject("id") 
                            .field("type", "long") 
                             .field("include_in_all",false)
                            .endObject()
                            .startObject("title")  
                                .field("type", "string") 
                                 .field("include_in_all",true)
                             .endObject()
                             .startObject("name")  
                             .field("type", "string")  
                             .field("include_in_all",true)
                             .endObject()
                   .endObject()  
                .endObject(); 
        mapping = mapping.endObject();
        // {"goods":{"_id":{"path":"id"},"properties":{"id":{"type":"long"},"title":{"type":"string","indexAnalyzer":"mmseg","searchAnalyzer":"mmseg"},"content":{"type":"string","indexAnalyzer":"mmseg","searchAnalyzer":"mmseg"}}}}
        System.out.println(mapping.string());
        PutMappingRequest mappingRequest = Requests.putMappingRequest(INDEX_NAME).type(TYPE_NAME).source(mapping);
        adminClientIndices.putMapping(mappingRequest).actionGet();
    }
    
}
