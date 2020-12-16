package com.databoy.app;

import com.databoy.udf.UserLogFilterFunction;
import com.databoy.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/16
 * @since 1.0.0
 */
public class OdsUserLogAppJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> userLogSource = env.addSource(new FlinkKafkaConsumer<String>("user-log", new SimpleStringSchema(), KafkaUtil.consumerProperties));

        SingleOutputStreamOperator<String> filterUserLogSource = userLogSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {

                return s.length() > 2 && s.startsWith("{") && s.endsWith("}");
            }
        });

        SingleOutputStreamOperator<String> pvSource = filterUserLogSource.filter(new UserLogFilterFunction("pv"));
        SingleOutputStreamOperator<String> showSource = filterUserLogSource.filter(new UserLogFilterFunction("show"));
        SingleOutputStreamOperator<String> clickSource = filterUserLogSource.filter(new UserLogFilterFunction("click"));
        SingleOutputStreamOperator<String> submitSource = filterUserLogSource.filter(new UserLogFilterFunction("submit"));

        pvSource.addSink(new FlinkKafkaProducer<String>("ods-log-pv",new SimpleStringSchema(),KafkaUtil.producerProperties));
        showSource.addSink(new FlinkKafkaProducer<String>("ods-log-show",new SimpleStringSchema(),KafkaUtil.producerProperties));
        clickSource.addSink(new FlinkKafkaProducer<String>("ods-log-click",new SimpleStringSchema(),KafkaUtil.producerProperties));
        submitSource.addSink(new FlinkKafkaProducer<String>("ods-log-submit",new SimpleStringSchema(),KafkaUtil.producerProperties));

        env.execute("OdsUserLogAppJob");
    }
}
