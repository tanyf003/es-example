package cn.tanyf.elasticsearch;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.Test;

public class MyTest extends BaseTestCase {

	@Test
	public void testDel() {
		//        DeleteRequest del = new DeleteRequestBuilder(client).request()
		//                .index("app").type("article").id("1");
		//        client.delete(del).actionGet();
		client.admin().indices().delete(new DeleteIndexRequest("app"));
		//        client.prepareDelete("app", "article", "1").execute().actionGet();
	}

	/**
	 * 
	 * <p>
	 * 创建索引
	 * </p>
	 * @author Geloin
	 * Created [2012-12-29 下午5:38:22]
	 * @throws Exception
	 */
	@Test
	public void createIndex() throws Exception {
		AdminClient adminClient = client.admin();
		IndicesAdminClient adminClientIndices = adminClient.indices();
		IndicesExistsRequest re = new IndicesExistsRequestBuilder(adminClientIndices, IndicesExistsAction.INSTANCE,
				"app").request();
		boolean flag = adminClientIndices.exists(re).actionGet().isExists();
		if (flag) {
			adminClientIndices.delete(new DeleteIndexRequest("app"));
		}

		// 预定义一个索引
		adminClientIndices.prepareCreate("app").execute().actionGet();

		// 定义索引字段属性
		XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
		mapping = mapping.startObject("article").startObject("properties").startObject("title").field("type", "string")
				.field("analyzer", "mmseg").endObject().endObject().endObject();
		mapping = mapping.endObject();

		PutMappingRequest mappingRequest = Requests.putMappingRequest("app").type("article").source(mapping);
		adminClientIndices.putMapping(mappingRequest).actionGet();

		// 生成文档
		XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
		doc = doc.field("title", "我是中国人 java");
		doc = doc.endObject();

		// 创建索引
		IndexResponse response = client.prepareIndex("app", "article", "1").setSource(doc).execute().actionGet();

		System.out.println(response.getId() + "====" + response.getIndex() + "====" + response.getType());
		client.close();
	}

	@Test
	public void search2() throws Exception {
		try {
			QueryBuilder qb = QueryBuilders.termsQuery("title", "中国人");
			//            QueryBuilder qb = QueryBuilders.queryString("java");
			SearchResponse scrollResp = client.prepareSearch("app").setQuery(qb).setSize(10).setFrom(0)
					.setExplain(true).highlighter(new HighlightBuilder().field("title", -1, 0)).execute().actionGet();
			String responseStr = scrollResp.toString();
			System.out.println(responseStr);
			SearchHits hits = scrollResp.getHits();
			System.out.println(hits.getTotalHits());
			for (SearchHit hit : hits.getHits()) {
				System.out.println(hit.getHighlightFields());
				String str = hit.getHighlightFields().get("title").fragments()[0].string();
				//                String str = hit.getSource().get("title").toString();
				System.out.println(str);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}