package cn.tanyf.elasticsearch.facets;

import cn.tanyf.elasticsearch.BaseTestCase;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * TODO: 增加描述
 *
 * @author hg_tyf@163.com
 * @version 0.1.0
 * @date 2014-8-22 下午5:31:32
 * @copyright yougou.com
 */
public class FacetQuery extends BaseTestCase {
    // 分组统计
    @Test
    public void topHits() throws ParseException {
        /**
         {
         "query" : {
         "query_string" : {
         "query" : "*"
         }
         },
         "aggregations" : {
         "top-tags" : {
         "terms" : {
         "field" : "sid1",
         "size" : 10,
         "min_doc_count" : 1,
         "order" : {
         "_term" : "asc"
         }
         }
         }
         }
         }

         * 高亮查询 必须指定"term_vector":"with_positions_offset"
         * {"_all":{"indexAnalyzer":"standard","searchAnalyzer":"standard","term_vector":"with_positions_offset","store":true,"enabled":true}
         * */
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = sdf.parse("2013-08-27");
        Date d2 = sdf.parse("2013-08-29");
        DateTime dt1 = new DateTime(d1.getTime(), DateTimeZone.UTC);
        DateTime dt2 = new DateTime(d2.getTime(), DateTimeZone.UTC);
        System.out.println(dt1.getMillis());
        QueryBuilder qb = QueryBuilders.queryStringQuery("拆迁")/*.analyzer("standard")*/;
        SearchRequestBuilder setQuery = client.prepareSearch(INDEX_NAME)
                .setTypes(TYPE_NAME)
                .setSearchType(SearchType.DEFAULT) // facets不返回hit，可以将SearchType设置为Count
                .addAggregation(AggregationBuilders.terms("top-tags").field("sid1")
                        .size(10)//指定统计的分组条数
                        .minDocCount(10) // 限制最小数目
//                      .order(Terms.Order.term(true)) // sex 升序排列
                        .order(Terms.Order.count(true)) // 数目 升序排列
                        // 子 分组  分页查询
//                      .collectMode(SubAggCollectionMode.BREADTH_FIRST)//depth_first breadth_first
                        .subAggregation(AggregationBuilders.topHits("top_tag_hits")
                                        .highlighter(new HighlightBuilder().field("title"))
//                    		  .setHighlighterFragmentSize(0)
//                    		  .setHighlighterNumOfFragments(100)

                    		  /*.setHighlighterQuery(QueryBuilders.termQuery("sid1", 0))
                              .setHighlighterFilter(true)*/
//                              .addSort("id", SortOrder.ASC)
                                        .fetchSource(true) // 查询组内所有字段
                                        .from(0)  // 分页
                                        .size(1) //此size制定 各组内的个数 1
                        )
                        .subAggregation(
                                //1
//                    		  AggregationBuilders.topHits("top_tag_hits") 
//                    		  .setHighlighterQuery(QueryBuilders.termQuery("sid1", 0))
//                    		  .setHighlighterFilter(true)
//                              .addSort("id", SortOrder.ASC)
//                              .setFetchSource(true) // 查询组内所有字段
//                              .setFrom(0)  // 分页
//                              .setSize(1) //此size制定 各组内的个数 1
                                // 2
                    		  /*AggregationBuilders.max("top_tag_hits").field("id") 查询每组 最大的id*/
                                // 3
//                    		  AggregationBuilders.dateHistogram("articles_over_time").field("_MASK_TO_V2").interval(Interval.MONTH)
//                    		  .format("yyyy-MM-dd").extendedBounds("2013-01-01", "2013-12-31").minDocCount(0L)
                                // 4
                                AggregationBuilders.dateRange("date_over_time").field("_MASK_TO_V2")
                                        .format("yyyy-MM-dd")
                                        .addRange("2013-01-01", "2013-04-01")
                                        .addRange("2013-04-01", "2013-07-01")
                                        .addRange("2013-07-01", "2013-10-01")
                        )
                        .subAggregation(
                                AggregationBuilders.range("date2_over_time").field("areacode")
                                        .addUnboundedFrom("areacode1", 210400)
                                        .addUnboundedTo("areacode1", 210499)
                                        .addUnboundedFrom("areacode2", 210500)
                                        .addUnboundedTo("areacode2", 210599)
                                        .addUnboundedFrom("areacode3", 210600)
                                        .addUnboundedTo("areacode3", 210699)
//              		  			.addRange(210400, 210499)
//              		  			.addRange(210500, 210599)
//              		  			.addRange(210600, 210699)
                        ))
//              .addAggregation(AggregationBuilders.topHits("sid2").setFrom(0).setSize(10))
                .setQuery(qb);
        System.out.println(setQuery.toString());
        SearchResponse response = setQuery
                .execute()
                .actionGet();
        System.out.println(response.toString());

        Aggregations f = response.getAggregations();
        //跟上面的名称一样
        for (Map.Entry<String, Aggregation> tf : f.asMap().entrySet()) {
            String key = tf.getKey();
            Aggregation value = tf.getValue();
            if (value instanceof InternalMin) {
                double min = ((InternalMin) value).getValue();
                System.out.println(key + "  " + min);
            } else if (value instanceof InternalValueCount) {
                long count = ((InternalValueCount) value).getValue();
                System.out.println(key + "  " + count);
            } else if (value instanceof InternalCardinality) {
                long cardinality = ((InternalCardinality) value).getValue();
                System.out.println(key + "  " + cardinality);
            } else if (value instanceof InternalGlobal) {
                long global = ((InternalGlobal) value).getDocCount();
                System.out.println(key + "  " + global);
            } else if (value instanceof InternalStats) {
                InternalStats stats = ((InternalStats) value);
                System.out.println("count" + "  " + stats.getCount());
                System.out.println("max" + "  " + stats.getMax());
                System.out.println("min" + "  " + stats.getMin());
                System.out.println("avg" + "  " + stats.getAvg());
                System.out.println("sum" + "  " + stats.getSum());
                System.out.println("name" + "  " + stats.getName());
            } else if (value instanceof InternalHistogram) {
                Collection histogramFacet = ((InternalHistogram) value).getBuckets();
                System.out.println(key + "  " + histogramFacet.iterator());
                Iterator iterator = histogramFacet.iterator();
                while (iterator.hasNext()) {
                    InternalHistogram.Bucket bucket = (InternalHistogram.Bucket) iterator.next();
                    System.out.println(bucket.getKey() + " : " + bucket.getDocCount());
                }
            } else if (value instanceof InternalHistogram) {
                List<InternalHistogram.Bucket> histogram = ((InternalHistogram) value).getBuckets();
                System.out.println(key + "  " + histogram);
                for (InternalHistogram.Bucket t : histogram) {
                    System.out.println(t.getKeyAsString() + "\t:\t" + t.getDocCount());
                }
            } else if (value instanceof InternalTopHits) {
                SearchHit[] hits = ((InternalTopHits) value).getHits().hits();
                if (null != hits && hits.length > 0) {
                    for (SearchHit hit : hits) {
                        // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                        System.out.println(hit.getSourceAsString());
                        Map<String, Object> fields = hit.getSource();
                        if (fields.containsKey("type")) {
                            System.out.println("type:" + fields.get("type"));
                        }
                        if (fields.containsKey("id")) {
                            System.out.println("id:" + fields.get("id"));
                        }
                    }
                }
            } else if (value instanceof InternalTerms) {
                Collection<Bucket> terms = ((InternalTerms) value).getBuckets();
                System.out.println(key + "  " + terms.iterator());
                Iterator iterator = terms.iterator();
                while (iterator.hasNext()) {
                    InternalTerms.Bucket bucket = (InternalTerms.Bucket) iterator.next();
                    System.out.println(bucket.getKey() + " : " + bucket.getDocCount());
                }
            }
        }
    }

    // 必须制定 lang 
    @Test
    public void statsScript() {
        QueryBuilder qb = QueryBuilders.queryStringQuery("*");
        Map map = new HashMap<String, Object>();
        map.put("correction", 1.2);
        SearchRequestBuilder setQuery = client.prepareSearch(INDEX_NAME)
                .setTypes(TYPE_NAME)
                .addAggregation(AggregationBuilders.stats("grades_stats").field("sid1")
                        .script(new Script("_value * correction")).setMetaData(map))
                .setQuery(qb);
        System.out.println(setQuery.toString());
        SearchResponse response = setQuery
                .execute()
                .actionGet();
        System.out.println(response.toString());

        Aggregations f = response.getAggregations();
        //跟上面的名称一样
        for (Map.Entry<String, Aggregation> tf : f.asMap().entrySet()) {
            String key = tf.getKey();
            Aggregation value = tf.getValue();
            if (value instanceof InternalMin) {
                double min = ((InternalMin) value).getValue();
                System.out.println(key + "  " + min);
            } else if (value instanceof InternalValueCount) {
                long count = ((InternalValueCount) value).getValue();
                System.out.println(key + "  " + count);
            } else if (value instanceof InternalCardinality) {
                long cardinality = ((InternalCardinality) value).getValue();
                System.out.println(key + "  " + cardinality);
            } else if (value instanceof InternalGlobal) {
                long global = ((InternalGlobal) value).getDocCount();
                System.out.println(key + "  " + global);
            } else if (value instanceof InternalStats) {
                InternalStats stats = ((InternalStats) value);
                System.out.println("count" + "  " + stats.getCount());
                System.out.println("max" + "  " + stats.getMax());
                System.out.println("min" + "  " + stats.getMin());
                System.out.println("avg" + "  " + stats.getAvg());
                System.out.println("sum" + "  " + stats.getSum());
                System.out.println("name" + "  " + stats.getName());
            } else if (value instanceof InternalHistogram) {
                Collection histogramFacet = ((InternalHistogram) value).getBuckets();
                System.out.println(key + "  " + histogramFacet.iterator());
                Iterator iterator = histogramFacet.iterator();
                while (iterator.hasNext()) {
                    InternalHistogram.Bucket bucket = (InternalHistogram.Bucket) iterator.next();
                    System.out.println(bucket.getKey() + " : " + bucket.getDocCount());
                }
            } else if (value instanceof InternalHistogram) {
                List<InternalHistogram.Bucket> histogram = ((InternalHistogram) value).getBuckets();
                System.out.println(key + "  " + histogram);
                for (InternalHistogram.Bucket t : histogram) {
                    System.out.println(t.getKeyAsString() + "\t:\t" + t.getDocCount());
                }
            } else if (value instanceof InternalTopHits) {
                SearchHit[] hits = ((InternalTopHits) value).getHits().getHits();
                if (null != hits && hits.length > 0) {
                    for (SearchHit hit : hits) {
                        // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                        System.out.println(hit.getSourceAsString());
                        Map<String, Object> fields = hit.getSource();
                        if (fields.containsKey("type")) {
                            System.out.println("type:" + fields.get("type"));
                        }
                        if (fields.containsKey("id")) {
                            System.out.println("id:" + fields.get("id"));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void stats() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequestBuilder setQuery = client.prepareSearch(INDEX_NAME)
                .setTypes(TYPE_NAME)
                .addAggregation(AggregationBuilders.stats("all_prices").field("price"))
                .setQuery(qb);
        System.out.println(setQuery.toString());
        SearchResponse response = setQuery
                .execute()
                .actionGet();
        System.out.println(response.toString());

        Aggregations f = response.getAggregations();
        //跟上面的名称一样
        for (Map.Entry<String, Aggregation> tf : f.asMap().entrySet()) {
            String key = tf.getKey();
            Aggregation value = tf.getValue();
            if (value instanceof InternalMin) {
                double min = ((InternalMin) value).getValue();
                System.out.println(key + "  " + min);
            } else if (value instanceof InternalValueCount) {
                long count = ((InternalValueCount) value).getValue();
                System.out.println(key + "  " + count);
            } else if (value instanceof InternalCardinality) {
                long cardinality = ((InternalCardinality) value).getValue();
                System.out.println(key + "  " + cardinality);
            } else if (value instanceof InternalGlobal) {
                long global = ((InternalGlobal) value).getDocCount();
                System.out.println(key + "  " + global);
            } else if (value instanceof InternalStats) {
                InternalStats stats = ((InternalStats) value);
                System.out.println("count" + "  " + stats.getCount());
                System.out.println("max" + "  " + stats.getMax());
                System.out.println("min" + "  " + stats.getMin());
                System.out.println("avg" + "  " + stats.getAvg());
                System.out.println("sum" + "  " + stats.getSum());
                System.out.println("name" + "  " + stats.getName());
            } else if (value instanceof InternalHistogram) {
                Collection histogramFacet = ((InternalHistogram) value).getBuckets();
                System.out.println(key + "  " + histogramFacet.iterator());
                Iterator iterator = histogramFacet.iterator();
                while (iterator.hasNext()) {
                    InternalHistogram.Bucket bucket = (InternalHistogram.Bucket) iterator.next();
                    System.out.println(bucket.getKey() + " : " + bucket.getDocCount());
                }
            } else if (value instanceof InternalHistogram) {
                List<InternalHistogram.Bucket> histogram = ((InternalHistogram) value).getBuckets();
                System.out.println(key + "  " + histogram);
                for (InternalHistogram.Bucket t : histogram) {
                    System.out.println(t.getKeyAsString() + "\t:\t" + t.getDocCount());
                }
            } else if (value instanceof InternalTopHits) {
                SearchHit[] hits = ((InternalTopHits) value).getHits().getHits();
                if (null != hits && hits.length > 0) {
                    for (SearchHit hit : hits) {
                        // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                        System.out.println(hit.getSourceAsString());
                        Map<String, Object> fields = hit.getSource();
                        if (fields.containsKey("type")) {
                            System.out.println("type:" + fields.get("type"));
                        }
                        if (fields.containsKey("id")) {
                            System.out.println("id:" + fields.get("id"));
                        }
                    }
                }
            }
        }
    }

    // 柱形图 interval 必须制定 例如 50  0~50  50~100 100~150
//    rem = value % interval
//            if (rem < 0) {
//                rem += interval
//            }
//            bucket_key = value - rem
    @Test
    public void histogram() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequestBuilder setQuery = client.prepareSearch(INDEX_NAME)
                .setTypes(TYPE_NAME)
                .addAggregation(AggregationBuilders.histogram("all_products").field("id").interval(527926322234894300L))
                .setQuery(qb);
        System.out.println(setQuery.toString());
        SearchResponse response = setQuery
                .execute()
                .actionGet();
        System.out.println(response.toString());

        Aggregations f = response.getAggregations();
        //跟上面的名称一样
        for (Map.Entry<String, Aggregation> tf : f.asMap().entrySet()) {
            String key = tf.getKey();
            Aggregation value = tf.getValue();
            if (value instanceof InternalMin) {
                double min = ((InternalMin) value).getValue();
                System.out.println(key + "  " + min);
            } else if (value instanceof InternalValueCount) {
                long count = ((InternalValueCount) value).getValue();
                System.out.println(key + "  " + count);
            } else if (value instanceof InternalCardinality) {
                long cardinality = ((InternalCardinality) value).getValue();
                System.out.println(key + "  " + cardinality);
            } else if (value instanceof InternalGlobal) {
                long global = ((InternalGlobal) value).getDocCount();
                System.out.println(key + "  " + global);
            } else if (value instanceof InternalHistogram) {
                Collection histogramFacet = ((InternalHistogram) value).getBuckets();
                System.out.println(key + "  " + histogramFacet.iterator());
                Iterator iterator = histogramFacet.iterator();
                while (iterator.hasNext()) {
                    InternalHistogram.Bucket bucket = (InternalHistogram.Bucket) iterator.next();
                    System.out.println(bucket.getKey() + " : " + bucket.getDocCount());
                }
            } else if (value instanceof InternalHistogram) {
                List<InternalHistogram.Bucket> histogram = ((InternalHistogram) value).getBuckets();
                System.out.println(key + "  " + histogram);
                for (InternalHistogram.Bucket t : histogram) {
                    System.out.println(t.getKeyAsString() + "\t:\t" + t.getDocCount());
                }
            } else if (value instanceof InternalTopHits) {
                SearchHit[] hits = ((InternalTopHits) value).getHits().hits();
                if (null != hits && hits.length > 0) {
                    for (SearchHit hit : hits) {
                        // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                        System.out.println(hit.getSourceAsString());
                        Map<String, Object> fields = hit.getSource();
                        if (fields.containsKey("type")) {
                            System.out.println("type:" + fields.get("type"));
                        }
                        if (fields.containsKey("id")) {
                            System.out.println("id:" + fields.get("id"));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void global() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequestBuilder setQuery = client.prepareSearch(INDEX_NAME)
                .setTypes(TYPE_NAME)
                .addAggregation(AggregationBuilders.global("all_products").subAggregation(AggregationBuilders.count("count").field("id")))
                .setQuery(qb);
        System.out.println(setQuery.toString());
        SearchResponse response = setQuery.get();
        System.out.println(response.toString());

        Aggregations f = response.getAggregations();
        //跟上面的名称一样
        for (Map.Entry<String, Aggregation> tf : f.asMap().entrySet()) {
            String key = tf.getKey();
            Aggregation value = tf.getValue();
            if (value instanceof InternalMin) {
                double min = ((InternalMin) value).getValue();
                System.out.println(key + "  " + min);
            } else if (value instanceof InternalValueCount) {
                long count = ((InternalValueCount) value).getValue();
                System.out.println(key + "  " + count);
            } else if (value instanceof InternalCardinality) {
                long cardinality = ((InternalCardinality) value).getValue();
                System.out.println(key + "  " + cardinality);
            } else if (value instanceof InternalGlobal) {
                long global = ((InternalGlobal) value).getDocCount();
                System.out.println(key + "  " + global);
            } else if (value instanceof InternalTopHits) {
                SearchHit[] hits = ((InternalTopHits) value).getHits().hits();
                if (null != hits && hits.length > 0) {
                    for (SearchHit hit : hits) {
                        // hit.getSourceAsString()，json格式 可以通过json工具直接转换为对应
                        System.out.println(hit.getSourceAsString());
                        Map<String, Object> fields = hit.getSource();
                        if (fields.containsKey("type")) {
                            System.out.println("type:" + fields.get("type"));
                        }
                        if (fields.containsKey("id")) {
                            System.out.println("id:" + fields.get("id"));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void facetsOldApiQuery() {
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("typeFacetName");
        aggregationBuilder.field("type").size(Integer.MAX_VALUE);
        SearchResponse response = client.prepareSearch(INDEX_NAME)
                .setTypes(TYPE_NAME)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(aggregationBuilder)
                .get();
        Aggregations f = response.getAggregations();
        //跟上面的名称一样
        StringTerms aggregation = (StringTerms) f.get("typeFacetName");
        System.out.println(aggregation);
        for (Terms.Bucket tf : aggregation.getBuckets()) {
            System.out.println(tf.getKey() + "\t:\t" + tf.getDocCount());
        }

//        type16 :   3
//        type12  :   2
//        type1   :   2
//        type4   :   1
//        type17  :   1
//        type13  :   1
    }

    protected String INDEX_NAME = "test_aggregation";
    protected String TYPE_NAME = "test_aggregation";

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
        adminClientIndices.prepareCreate(INDEX_NAME).setSettings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)).get();
        InputStream is = Streams.class.getResourceAsStream("/mapping/aggregation/aggregation.json");
        String mapping = Streams.copyToString(new InputStreamReader(is));
        PutMappingRequest mappingRequest = Requests.putMappingRequest(INDEX_NAME).type(TYPE_NAME).source(mapping, XContentFactory.xContentType(mapping));
        adminClientIndices.putMapping(mappingRequest).actionGet();
    }

    @Test
    public void bulkCreateIndex() {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (int i = 1; i <= 10; i++) {
            FacetTestModel model = new FacetTestModel();
            model.setId(i);
            String json = JSON.toJSONString(model);
            IndexRequestBuilder indexRequest = client.prepareIndex(INDEX_NAME, TYPE_NAME)
                    .setSource(json, XContentType.JSON).setId(String.valueOf(i));
            //添加到builder中
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            System.out.println(bulkResponse.buildFailureMessage());
        }
    }

}
