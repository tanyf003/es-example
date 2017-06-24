package cn.tanyf.elasticsearch.count;

import cn.tanyf.elasticsearch.BaseTestCase;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.util.List;

/**
 * 
 * 查询返回条数
 * @author tan.yf
 * @date 2017年5月26日 上午10:37:50
 * @version 0.1.0 
 * @copyright wonhigh.cn
 */
public class CountTest extends BaseTestCase {
	private boolean EXPLAIN = true; // 类似 sql explain

	/***
	 * 
		GET /goods/goods/_search
		{
		  "query": {
		    "multi_match": {
		      "query": "代价伤痕2dd0dwxx012",
		      "fields": [
		        "content",
		        "title"
		      ],
		      "minimum_should_match": "49%",
		      "type": "best_fields",
		      "operator": "OR"
		    }
		  },
		  "explain": true,
		  "size": 20
		}
	 */
	@Test
	public void count() {
		String keyword ="代价伤痕2";
		keyword = QueryParser.escape(keyword);
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		AnalyzeRequestBuilder request = new AnalyzeRequestBuilder(indicesAdminClient,AnalyzeAction.INSTANCE,INDEX_NAME,keyword);
		request.setAnalyzer("query_ansj");
//		request.setTokenizer("index_ansj");
		// Analyzer（分析器）、Tokenizer（分词器）
		List<AnalyzeToken> listAnalysis = request.get().getTokens();
		for (AnalyzeToken token : listAnalysis){
			System.out.println(token.getPosition() + "," + token.getTerm());
		}
		// Operator.AND 表示每个词元都匹配
		// minimumShouldMatch 匹配百分比,50%  关键词分词后（n 个词 ） n*50% 向下取整数 得 x，必须命中 x 个
//		QueryBuilder qb = QueryBuilders.termQuery("content", "代价");
		QueryBuilder qb = QueryBuilders.multiMatchQuery(keyword, "content","title")
									   .minimumShouldMatch("39%")
									   .operator(Operator.OR)
									   .type(Type.BEST_FIELDS)
									   /*.tieBreaker(0.3f)*/;
		SearchRequestBuilder req = client.prepareSearch(INDEX_NAME)
										.setTypes(TYPE_NAME)
										 .storedFields("content","title")
									    .setSource(new SearchSourceBuilder().size(10)
									    .query(qb))
//									    .setMinScore(0.03f)
									    .setExplain(EXPLAIN);
		System.out.println(" SearchRequestBuilder ===> " + req);
		SearchResponse response = req.get();
		System.out.println(response.getHits().getTotalHits() + ",maxscore: " + response.getHits().getMaxScore());
		for (SearchHit searchHit : response.getHits().getHits()){
			System.out.println(searchHit.getExplanation());
			System.out.println(searchHit.getSourceAsString()) ;
		}
	}
}
