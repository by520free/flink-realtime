package com.databoy.app;

import com.databoy.udf.PhoenixSinkFunction;
import com.databoy.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/14
 * @since 1.0.0
 */
public class DwdMeidaAppJob {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> mediaSource = env.addSource(new FlinkKafkaConsumer<String>("ods-media", new SimpleStringSchema(), KafkaUtil.consumerProperties));

        mediaSource.addSink(new PhoenixSinkFunction("media"));

        env.execute("DwdMeidaAppJob");
    }
}
