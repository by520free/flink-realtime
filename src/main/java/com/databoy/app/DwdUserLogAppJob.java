package com.databoy.app;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.databoy.udf.UserLogJoinMapFunction;
import com.databoy.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.regex.Pattern;

/**
 * 〈一句话功能简述）
 * 〈关联维度表〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/16
 * @since 1.0.0
 */
public class DwdUserLogAppJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Pattern topicPattern = java.util.regex.Pattern.compile("ods-log-.+");
        DataStreamSource<String> userLogSource = env.addSource(new FlinkKafkaConsumer<String>(topicPattern, new SimpleStringSchema(), KafkaUtil.consumerProperties));

        SingleOutputStreamOperator<String> joinedUserLogStream = userLogSource.map(new UserLogJoinMapFunction());

        // 分流
        joinedUserLogStream.filter(json -> {
            JSONObject jsonObject = JSONUtil.parseObj(json);

            return "pv".equals(jsonObject.getStr("type"));
        }).addSink(new FlinkKafkaProducer<String>("dwd-log-pv",new SimpleStringSchema(),KafkaUtil.producerProperties));

        joinedUserLogStream.filter(json ->{

            JSONObject jsonObject = JSONUtil.parseObj(json);

            return "show".equals(jsonObject.getStr("type"));
        }).addSink(new FlinkKafkaProducer<String>("dwd-log-show",new SimpleStringSchema(),KafkaUtil.producerProperties));

        joinedUserLogStream.filter(json ->{

            JSONObject jsonObject = JSONUtil.parseObj(json);

            return "click".equals(jsonObject.getStr("type"));
        }).addSink(new FlinkKafkaProducer<String>("dwd-log-click",new SimpleStringSchema(),KafkaUtil.producerProperties));

        joinedUserLogStream.filter(json ->{

            JSONObject jsonObject = JSONUtil.parseObj(json);

            return "submit".equals(jsonObject.getStr("type"));
        }).addSink(new FlinkKafkaProducer<String>("dwd-log-submit",new SimpleStringSchema(),KafkaUtil.producerProperties));

        env.execute("DwdUserLogAppJob");
    }
}
