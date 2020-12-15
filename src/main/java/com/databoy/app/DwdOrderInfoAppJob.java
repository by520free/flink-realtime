package com.databoy.app;

import cn.hutool.json.JSONUtil;
import com.databoy.beans.OrderInfo;
import com.databoy.beans.OrderInfoDetail;
import com.databoy.udf.FirstOrderFilterFunction;
import com.databoy.udf.OrderInfoJoinMapFunction;
import com.databoy.utils.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.app
 * @Description: TODO
 * @date 2020/12/14 23:03
 */
public class DwdOrderInfoAppJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

        DataStreamSource<String> orderInfoSource = env.addSource(new FlinkKafkaConsumer<String>("ods-orderinfo", new SimpleStringSchema(), KafkaUtil.consumerProperties));


        SingleOutputStreamOperator<OrderInfoDetail> joinedOrderInfo = orderInfoSource.map(json -> {

            OrderInfo orderInfo = JSONUtil.toBean(json, OrderInfo.class);
            return orderInfo;
        }).map(new OrderInfoJoinMapFunction());


        SingleOutputStreamOperator<String> firstOrderStream = joinedOrderInfo.keyBy("customerId")
                .timeWindow(Time.seconds(1))
                .min("createTime")
                .filter(new FirstOrderFilterFunction())
                .map(new MapFunction<OrderInfoDetail, String>() {
                    @Override
                    public String map(OrderInfoDetail orderInfoDetail) throws Exception {

                        return JSONUtil.toJsonStr(orderInfoDetail);
                    }
                });

        firstOrderStream.print();
//
//        List<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("172.26.13.85", 9200, "http"));
//
//        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
//        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
//                httpHosts,
//                new ElasticsearchSinkFunction<String>() {
//                    public IndexRequest createIndexRequest(String element) {
//                        Map<String, String> json = new HashMap<>();
//                        json.put("data", element);
//
//                        return Requests.indexRequest()
//                                .index("first-order")
//                                .type("_doc")
//                                .source(json);
//                    }
//
//                    @Override
//                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                        indexer.add(createIndexRequest(element));
//                    }
//                }
//        );
//        esSinkBuilder.setBulkFlushMaxActions(1);
//        firstOrderStream.addSink(esSinkBuilder.build());
//
//        joinedOrderInfo.map(new MapFunction<OrderInfoDetail, String>() {
//            @Override
//            public String map(OrderInfoDetail orderInfoDetail) throws Exception {
//
//                return JSONUtil.toJsonStr(orderInfoDetail);
//            }
//        }).addSink(new FlinkKafkaProducer<String>("dwd-orderdetail",new SimpleStringSchema(),KafkaUtil.producerProperties));

        env.execute("DwdOrderInfoAppJob");
    }
}
