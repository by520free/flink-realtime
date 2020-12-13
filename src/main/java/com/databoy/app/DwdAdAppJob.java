package com.databoy.app;

import com.databoy.beans.AdBean;
import com.databoy.udf.PhoenixSinkFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.app
 * @Description: TODO
 * @date 2020/12/13 21:59
 */
public class DwdAdAppJob {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master01:9092,slave01:9092,slave02:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> adSource = env.addSource(new FlinkKafkaConsumer<String>("ods-ad", new SimpleStringSchema(), properties));

        adSource.map(new MapFunction<String, AdBean>() {
            @Override
            public AdBean map(String s) throws Exception {

                AdBean adBean = new AdBean();

                return adBean;
            }
        }).addSink(new SinkFunction<AdBean>() {
            @Override
            public void invoke(AdBean value, Context context) throws Exception {


            }
        });

        env.execute("DwdAdAppJob");
    }
}
