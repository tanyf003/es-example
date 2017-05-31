package cn.tanyf.elasticsearch.delete;


import cn.tanyf.elasticsearch.BaseTestCase;
import org.elasticsearch.action.delete.DeleteResponse;
import org.junit.Test;

/**
 * 
 * 创建索引
 * @author hg_tyf@163.com
 */
public class DeleteIndexTest extends BaseTestCase{
    private String ID="1";
    @Test
    public void prepareDeleteTest(){
        DeleteResponse response = client.prepareDelete(INDEX_NAME, TYPE_NAME,ID)
//        								.setVersion(1)
//        								.setVersionType(VersionType.INTERNAL)
        								.get();
        response.status().name();
        response.getResult().name();
    }
}
