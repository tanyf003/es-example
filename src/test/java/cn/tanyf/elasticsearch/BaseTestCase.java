package cn.tanyf.elasticsearch;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

public class BaseTestCase {
	protected TransportClient client;
    private String clusterNodes = "s100:9300";
	private String clusterName = "elasticsearch";
	private Boolean clientIgnoreClusterName = Boolean.FALSE;
	private String clientPingTimeout = "5s";
	private String clientNodesSamplerInterval = "5s";
    private Properties properties;
    static final String COLON = ":";
	static final String COMMA = ",";
    private Settings settings() {
		if (properties != null) {
			return Settings.builder().put(properties).build();
		}
		return Settings.builder()
				.put("cluster.name", clusterName)
				.put("xpack.security.transport.ssl.enabled", false)
                .put("xpack.security.user", "elastic:changeme")
				.put("client.transport.sniff", true)
				.put("client.transport.ignore_cluster_name", clientIgnoreClusterName)
				.put("client.transport.ping_timeout", clientPingTimeout)
				.put("client.transport.nodes_sampler_interval", clientNodesSamplerInterval)
				.build();
	}
    @Before
    public void setUp() throws Exception {
       client = new PreBuiltXPackTransportClient(settings());
	   	for (String clusterNode : StringUtils.split(clusterNodes, COMMA)) {
			String hostName = StringUtils.substringBeforeLast(clusterNode, COLON);
			String port = StringUtils.substringAfterLast(clusterNode, COLON);
			System.out.println("hostname: " + hostName + ", port: " + port);
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port)));
		}
	   	client.connectedNodes();
    }
    @After
    public void tearDown() {
        client.close();
    }
    
    protected String INDEX_NAME="test_goods";
    protected String TYPE_NAME="test_goods";
    /** 创建索引结构  类似 sql 建表语句*/
    @Test
    public void putMapping() throws IOException{
    	System.out.println(client);
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
                	.startObject("_all").field("enabled", false).endObject()
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
        System.out.println(mapping.string());
        PutMappingRequest mappingRequest = Requests.putMappingRequest(INDEX_NAME).type(TYPE_NAME).source(mapping);
        adminClientIndices.putMapping(mappingRequest).actionGet();
    }
    
    @Test
    public void addData() throws Exception{
       IndexResponse indexResponse = client
               .prepareIndex(INDEX_NAME, TYPE_NAME,"2")
               .setSource(XContentFactory.jsonBuilder().startObject()
                               .field("id",1)
                               .field("title", "宁伤痕为2")
                               .field("content", "kimchy elasticsearch 因为带有以宁伤痕为代价的美丽风景总是让人不由地惴惴不安，在myboy紧接着袭面而来的抑或是病痛抑或是灾难，没有谁会能够安逸着恬然，myboy因为模糊让人撕心裂肺地想呐喊。")
                               .endObject())
                               .execute().actionGet();
       System.out.println(indexResponse.getResult());
    }
    
    /**批量*/
    @Test
    public void bulkData() throws Exception{
       BulkRequest bulkRequest = new BulkRequest();
       for (int i = 1; i <= 10000; i++){
    	   bulkRequest.add(new IndexRequest(INDEX_NAME, TYPE_NAME, i + "")
    	   			.source(XContentFactory.jsonBuilder().startObject()
										                  .field("id",i)
										                  .field("title", "宁伤痕为"+i)
										                  .field("content", i + "kimchy elasticsearch 因为带有以宁伤痕为代价的美丽风景总是让人不由地惴惴不安，在myboy紧接着袭面而来的抑或是病痛抑或是灾难，没有谁会能够安逸着恬然，myboy因为模糊让人撕心裂肺地想呐喊。")
										                  .endObject())
										                  );
       }
       BulkResponse bulkResponse = client.bulk(bulkRequest).get();
       System.out.println(bulkResponse.status().name());
    }
}
