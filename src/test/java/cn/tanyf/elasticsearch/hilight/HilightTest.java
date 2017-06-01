package cn.tanyf.elasticsearch.hilight;

import cn.tanyf.elasticsearch.BaseTestCase;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;

public class HilightTest extends BaseTestCase
{
    public static final String INDEX_NAME="test_hilight";
    public static final String TYPE_NAME="test_hilight";

    @Test
    public void testQuery()
    {
       //q为要查询的数据
//         QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("因为");
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME,TYPE_NAME)
                .setQuery(QueryBuilders.queryStringQuery("因为")).setSize(10).setFrom(0).setExplain(true).setProfile(true)
                 .highlighter(new HighlightBuilder().field("title",-1,0)
                                                    .preTags("<span style='color:red;'>")
                                                     .postTags("</span>"))
                 .highlighter(new HighlightBuilder().field("intro",-1,0)
                                    .preTags("<span style='color:red;'>")
                                    .postTags("</span>")
                                )
                 .get();
        String responseStr = searchResponse.toString();
        System.out.println(responseStr);
        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.getTotalHits());
        for (SearchHit hit : hits.getHits())
        {
             String str =
             hit.getHighlightFields().get("intro").fragments()[0].string();
//            String str = hit.getSource().get("title").toString();
            System.out.println(str);
        }
        // for (int i=0;i<hits.hits().length;i++){
        // System.out.println(hits.getAt(i).highlightFields().get("title").fragments()[0].string());
        // }
    }

    @Test
    public void addData() throws Exception
    {
//    	http://localhost:9200/test_hilight/_analyze?analyzer=index_ansj&text=测试elasticsearch分词器
        for (int i = 0; i < 10 ; i++){
            IndexResponse indexResponse = client
                    .prepareIndex(INDEX_NAME, TYPE_NAME, i + "")
                    //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
                    .setSource(XContentFactory.jsonBuilder().startObject()
                                    .field("id",i + "")
                                    .field("title", "不锈钢")
                                    .field("intro", "因为带有以宁伤痕为代价的美丽风景总是让人不由地惴惴不安，在myboy紧接着袭面而来的抑或是病痛抑或是灾难，没有谁会能够安逸着恬然，myboy因为模糊让人撕心裂肺地想呐喊。")
                                    .endObject()).get();
            System.out.println(indexResponse.getId());
        }
    }

    @Test
    public void query()
    {
         String keyWord = "因为带有以宁伤痕为代价的美丽风景总是让人";
//       String keyWord = "老年 高血压";  
//       String keyWord = "gxy";  
        //多个字段匹配  
//        MultiMatchQueryBuilder query = QueryBuilders.multiMatchQuery(keyWord, "intro");  
//         QueryStringQueryBuilder query = new QueryStringQueryBuilder("因为");
        long b = System.currentTimeMillis();  
       SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes(TYPE_NAME)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
               .setQuery(QueryBuilders.commonTermsQuery("intro", keyWord))
               .setSize(10).setFrom(0).setExplain(true)
               .highlighter(new HighlightBuilder().field("intro",20,5))
               .get();
        long useTime = System.currentTimeMillis()-b;  
        System.out.println("search use time:" + useTime + " ms");  
          
        SearchHits shs = response.getHits();  
        System.out.println(shs.getTotalHits());
        for (SearchHit hit : shs) {  
            System.out.println("分数:"  
                    + hit.getScore()  
                    + ",ID:"  
                    + hit.getId()  
                    + ", title:"  
                    + hit.getSource().get("title")  
                    + ",intro:" + hit.getHighlightFields().get("intro").fragments()[0].string());  
        }  
    }

    @Test
    public void putxMapping() throws Exception {
        AdminClient adminClient = client.admin();
        IndicesAdminClient adminClientIndices = adminClient.indices();
        IndicesExistsRequest re = new IndicesExistsRequestBuilder(adminClientIndices, IndicesExistsAction.INSTANCE, INDEX_NAME).request();
        boolean flag = adminClientIndices.exists(re).actionGet().isExists();
        if (flag) {
            adminClientIndices.delete(new DeleteIndexRequest(INDEX_NAME));
        }
        // 预定义一个索引
        adminClientIndices.prepareCreate(INDEX_NAME).execute().actionGet();
        InputStream is = Streams.class.getResourceAsStream("/mapping/hilight/hilight.json");
        String mapping = Streams.copyToString(new InputStreamReader(is));
        PutMappingRequest mappingRequest = Requests.putMappingRequest(INDEX_NAME).type(TYPE_NAME).source(mapping, XContentFactory.xContentType(mapping));
        adminClientIndices.putMapping(mappingRequest).actionGet();
    }
}
