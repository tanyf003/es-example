package cn.tanyf.elasticsearch.update;


import cn.tanyf.elasticsearch.BaseTestCase;
import com.beust.jcommander.internal.Maps;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.testng.annotations.Test;

import java.util.Map;

/**
 * 创建索引
 */
public class UpdateIndexTest extends BaseTestCase {
    private String ID = "1111111";

    @Test
    public void prepareUpdateTest() {
        Map<String,String> map = Maps.newHashMap();
        map.put("title","2b");// 只修改 title 类似 sql, update doc set title=? where id=1
        map.put("content","2b");
        UpdateResponse response = client.prepareUpdate(INDEX_NAME, TYPE_NAME, ID)
                                        .setDoc(map)
                                        .setDocAsUpsert(true) // 不存在则插入，存在则更新
//        								.setVersion(1)
//        								.setVersionType(VersionType.INTERNAL)
                                        .get();
        String statusName = response.status().name();
        String resultName = response.getResult().name();
        System.out.println(statusName + "," + resultName);
    }

    @Test
    public void updateByQuery() {
        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                                            .filter(QueryBuilders.matchQuery("id", "16"))
                                            .source(INDEX_NAME)
                                            .refresh(true)
                                            .get();
        // 返回行数
        long updated = response.getUpdated();
        System.out.println(updated);
    }

    @Test
    public void updateByQueryActionListener() {
        UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("id", "15"))
                .source(INDEX_NAME)
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long updated = response.getUpdated();
                        System.out.println(updated);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        e.printStackTrace();
                    }
                });
    }
}
