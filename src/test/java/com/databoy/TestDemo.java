package com.databoy;

import cn.hutool.json.JSONUtil;
import com.databoy.beans.Customer;
import com.databoy.udf.MyEsSinkFunction;
import com.databoy.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/14
 * @since 1.0.0
 */
public class TestDemo {

    @Test
    public void testOne(){

        String str = "{\"id\":37,\"customer_id\":\"6vzp5yk9yc\",\"name\":\"南京萌新健身\",\"city\":\"南京\",\"desc\":\"萌新健身有限公司\"}";

        Customer customer = JSONUtil.toBean(str, Customer.class);

        System.out.println(customer.getCustomerId());
    }


    @Test
    public void tableDemo() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("ods-orderinfo", new SimpleStringSchema(), KafkaUtil.consumerProperties));

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.26.13.85", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,new MyEsSinkFunction());
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        dataStreamSource.addSink(esSinkBuilder.build());

        env.execute("tableDemo");
    }
}
