package cn.tanyf.elasticsearch;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.Test;

public class HilightTest extends BaseTestCase
{
	static final String INDEX_NAME="hilight";  
	static final String TYPE_NAME="test"; 

    @Test
    public void TestQuery()
    {
       //q为要查询的数据
//         QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("因为");
        SearchResponse searchResponse = client.prepareSearch("hilight7")
                .setQuery(QueryBuilders.queryStringQuery("因为")).setSize(10).setFrom(0).setExplain(true)
                 .highlighter(new HighlightBuilder().field("title",-1,0))
                 .highlighter(new HighlightBuilder().field("intro",-1,0))
                // 增加高亮显示的HTML
                /*
                 * .setHighlighterPreTags("<span style='color:red;'>")
                 * .setHighlighterPostTags("</span>")
                 */.execute().actionGet();
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
    public void createIndex() throws Exception
    {
//    	http://localhost:9200/hilight/_analyze?analyzer=mmseg&text=测试elasticsearch分词器
		IndexResponse indexResponse = client
                .prepareIndex("hilight3", "test", "2")
                //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
                .setSource(
                        XContentFactory.jsonBuilder().startObject()
                                .field("title", "不锈钢")
                                .field("intro", "因为带有以宁伤痕为代价的美丽风景总是让人不由地惴惴不安，在myboy紧接着袭面而来的抑或是病痛抑或是灾难，没有谁会能够安逸着恬然，myboy因为模糊让人撕心裂肺地想呐喊。")
                                .endObject()).execute().actionGet();
        System.out.println(indexResponse.getId());
    }
    
    
    @Test
    public void testQuery2()
    {
         String keyWord = "因为";  
//       String keyWord = "老年 高血压";  
//       String keyWord = "gxy";  
        //多个字段匹配  
//        MultiMatchQueryBuilder query = QueryBuilders.multiMatchQuery(keyWord, "intro");  
//         QueryStringQueryBuilder query = new QueryStringQueryBuilder("因为");
        long b = System.currentTimeMillis();  
       SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes(TYPE_NAME)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
               .setQuery(QueryBuilders.commonTermsQuery("intro", "因为"))
               .setSize(10).setFrom(0).setExplain(true)
               .highlighter(new HighlightBuilder().field("intro",-1,0))
               .execute().actionGet();
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
        client.close();
    }  
    
    @Test
    public void createIndex2() throws Exception
    {
//         Builder settings = ImmutableSettings.settingsBuilder()  
//                 .loadFromSource(XContentFactory.jsonBuilder().startObject()
//                       .field("title", "不锈钢")
//                       .field("intro", "因为带有以宁伤痕为代价的美丽风景总是让人不由地惴惴不安，在myboy紧接着袭面而来的抑或是病痛抑或是灾难，没有谁会能够安逸着恬然，myboy因为模糊让人撕心裂肺地想呐喊。")
//                       .endObject()
//                       .toString()); 
//         //首先创建索引库  
//         CreateIndexResponse  indexresponse = client.admin().indices()  
//         //这个索引库的名称还必须不包含大写字母  
//         .prepareCreate(INDEX_NAME).setSettings(settings)  
//         //这里直接添加type的mapping  
//         .addMapping(TYPE_NAME, getMapping())  
//         .execute().actionGet();  
         IndexResponse indexResponse = client
                 .prepareIndex(INDEX_NAME,TYPE_NAME)
                 .setSource(XContentFactory.jsonBuilder().startObject()
                                 .field("title", "不锈钢")
                                 .field("intro", "因为带有以宁伤痕为代价的美丽风景总是让人不由地惴惴不安，在myboy紧接着袭面而来的抑或是病痛抑或是灾难，没有谁会能够安逸着恬然，myboy因为模糊让人撕心裂肺地想呐喊。")
                                 .endObject()).execute().actionGet();
         System.out.println(indexResponse.getId());
    }
}
