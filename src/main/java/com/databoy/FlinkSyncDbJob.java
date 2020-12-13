package com.databoy;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.databoy.beans.DbDataBean;
import com.databoy.udf.DbDataBean2JsonMapFunction;
import com.databoy.udf.TableFilterFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Properties;

/**
 * 〈一句话功能简述）
 * 〈同步mysql表到hbase〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/12
 * @since 1.0.0
 */
public class FlinkSyncDbJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取kafka数据源
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master01:9092,slave01:9092,slave02:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        DataStreamSource<String> dbSource = env.addSource(new FlinkKafkaConsumer<String>("maxwell-mydb", new SimpleStringSchema(), properties));

        // ad、customer、flow、media、user、orderinfo
        // insert、update、bootstrap-insert
        SingleOutputStreamOperator<DbDataBean> filterDbSource = dbSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {

                boolean flag = false;
                if (s.length() >= 2) {

                    // {"database":"mydb","table":"customer","type":"bootstrap-insert","ts":1607666868,"data":{"id":37,"customer_id":"6vzp5yk9yc","name":"南京萌新健身","city":"南京","desc":"萌新健身有限公司"}}
                    JSONObject logJson = JSONUtil.parseObj(s);
                    String database = logJson.getStr("database");
                    String type = logJson.getStr("type");
                    flag = database.equals("mydb");
                    flag = flag && (type.contains("insert") || type.equals("update"));

                }


                return flag;
            }
        }).map(new MapFunction<String, DbDataBean>() {
            @Override
            public DbDataBean map(String s) throws Exception {

                JSONObject logJson = JSONUtil.parseObj(s);
                DbDataBean dbDataBean = new DbDataBean();
                dbDataBean.setDatabase(logJson.getStr("database"));
                dbDataBean.setTable(logJson.getStr("table"));
                dbDataBean.setType(logJson.getStr("type"));
                dbDataBean.setData(logJson.getJSONObject("data"));

                return dbDataBean;
            }
        });

        Properties producerProperties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop100:9092,hadoop101:9092,hadoop102:9092");

        // 过滤出各个表的数据
        SingleOutputStreamOperator<String> adTableSource = filterDbSource.filter(new TableFilterFunction("ad")).map(new DbDataBean2JsonMapFunction());
        SingleOutputStreamOperator<String> mediaTableSource = filterDbSource.filter(new TableFilterFunction("media")).map(new DbDataBean2JsonMapFunction());
        SingleOutputStreamOperator<String> flowTableSource = filterDbSource.filter(new TableFilterFunction("flow")).map(new DbDataBean2JsonMapFunction());
        SingleOutputStreamOperator<String> customerTableSource = filterDbSource.filter(new TableFilterFunction("customer")).map(new DbDataBean2JsonMapFunction());
        SingleOutputStreamOperator<String> userTableSource = filterDbSource.filter(new TableFilterFunction("user")).map(new DbDataBean2JsonMapFunction());
        SingleOutputStreamOperator<String> orderinfoTableSource = filterDbSource.filter(new TableFilterFunction("orderinfo")).map(new DbDataBean2JsonMapFunction());

        // 分流到dwd层topic
        adTableSource.addSink(new FlinkKafkaProducer<String>("ods-ad",new SimpleStringSchema(),producerProperties));
        mediaTableSource.addSink(new FlinkKafkaProducer<String>("ods-media",new SimpleStringSchema(),producerProperties));
        flowTableSource.addSink(new FlinkKafkaProducer<String>("ods-flow",new SimpleStringSchema(),producerProperties));
        customerTableSource.addSink(new FlinkKafkaProducer<String>("ods-customer",new SimpleStringSchema(),producerProperties));
        userTableSource.addSink(new FlinkKafkaProducer<String>("ods-user",new SimpleStringSchema(),producerProperties));
        orderinfoTableSource.addSink(new FlinkKafkaProducer<String>("ods-orderinfo", new SimpleStringSchema(),producerProperties));


        env.execute("FlinkSyncDbJob");
    }
}