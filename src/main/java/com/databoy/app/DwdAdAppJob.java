package com.databoy.app;

import com.databoy.udf.PhoenixSinkFunction;
import com.databoy.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.app
 * @Description: TODO
 * @date 2020/12/13 21:59
 */
public class DwdAdAppJob {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> adSource = env.addSource(new FlinkKafkaConsumer<String>("ods-ad", new SimpleStringSchema(), KafkaUtil.consumerProperties));

        adSource.addSink(new PhoenixSinkFunction("ad"));

        env.execute("DwdAdAppJob");
    }
}
