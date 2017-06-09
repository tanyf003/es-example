### ansj 分词器
1. /_cat/ansj: 执行分词
2. /_cat/ansj/config: 显示全部配置
3. /_ansj/flush/config: 刷新全部配置
4. /_ansj/flush/dic: 更新全部词典。包括用户自定义词典,停用词典,同义词典,歧义词典,crf
(例如：/_ansj/flush/dic?key=synonyms) 

### 假设已经安装了es 和 kibana，现在安装x-pack
```bash
bin/elasticsearch-plugin install file:/usr/local/x-pack-5.4.0.zip
```
```bash
bin/kibana-plugin install file:/usr/local/x-pack-5.4.0.zip
```

###  x-pack 安全，客户端连接es
####test 用户只有 test_* 开头的索引权限（在kibana 中设置用户账号和权限）
```java 
Settings.builder()
        .put("cluster.name", clusterName)
        .put("xpack.security.transport.ssl.enabled", false)
        .put("xpack.security.user", "test:test123456")
        .put("client.transport.sniff", true)
        .put("client.transport.ignore_cluster_name", clientIgnoreClusterName)
        .put("client.transport.ping_timeout", clientPingTimeout)
        .put("client.transport.nodes_sampler_interval", clientNodesSamplerInterval)
        .build()
```



