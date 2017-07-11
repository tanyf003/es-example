package cn.tanyf.elasticsearch.delete;


import cn.tanyf.elasticsearch.BaseTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.testng.annotations.Test;

/**
 * 创建索引
 *
 */
public class DeleteIndexTest extends BaseTestCase {
    private String ID = "1";

    @Test
    public void prepareDeleteTest() {
        DeleteResponse response = client.prepareDelete(INDEX_NAME, TYPE_NAME, ID)
//        								.setVersion(1)
//        								.setVersionType(VersionType.INTERNAL)
                .get();
        response.status().name();
        response.getResult().name();
    }

    @Test
    public void deleteByQuery() {
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.matchQuery("id", "16"))
                        .source(INDEX_NAME)
                        .refresh(true)
                        .get();

        // 返回影响行数
        long deleted = response.getDeleted();
        System.out.println(deleted);
    }

    @Test
    public void deleteByQueryActionListener() {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("id", "15"))
                .source(INDEX_NAME)
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                        System.out.println(deleted);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        e.printStackTrace();
                    }
                });
    }
}
