package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task11;

import com.example.demo.bigdata.tutorial.flink.common.EventGenerator;
import com.example.demo.bigdata.tutorial.flink.common.FlinkEvent;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description: sink es
 * @Author: Chenyang on 2024/10/21 18:31
 * @Version: 1.0
 */
public class FlinkSinkTest4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<FlinkEvent> dataStream = env.fromCollection(EventGenerator.getFlinkEventList());

        // 配置es服务器信息
        List<HttpHost> httpHosts = Arrays.asList(new HttpHost("127.0.0.1", 9200));

        // 定义ElasticSearchSinkFunction
        ElasticsearchSinkFunction<FlinkEvent> esSinkFunction = (flinkEvent, runtimeContext, requestIndexer) -> {
            Map<String, String> map = new HashMap<String, String>() {{
                put(flinkEvent.getName(), flinkEvent.toString());
            }};

            // 构建一个IndexRequest
            IndexRequest request = Requests.indexRequest()
                    .index("from-flink")
                    .type("_doc")
                    .source(map);

            requestIndexer.add(request);
        };

        dataStream.addSink(new ElasticsearchSink.Builder<>(httpHosts, esSinkFunction).build());

        System.out.println("=====任务提交=====");
        env.execute();
    }
}
