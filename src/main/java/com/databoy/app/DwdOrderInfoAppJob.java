package com.databoy.app;

import cn.hutool.json.JSONObject;
import com.databoy.utils.KafkaUtil;
import com.sun.prism.PixelFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.Map;

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
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        TableSchema orderInfoSchema = TableSchema.builder()
                .field("id", DataTypes.INT())
                .field("orderId", DataTypes.STRING())
                .field("customerId", DataTypes.STRING())
                .field("adId", DataTypes.STRING())
                .field("mediaId", DataTypes.STRING())
                .field("flowId", DataTypes.STRING())
                .field("count", DataTypes.BIGINT())
                .field("createTime", DataTypes.STRING())
                .field("status", DataTypes.STRING())
                .build();

        new KafkaTableSource(orderInfoSchema,"ods-orderinfo",KafkaUtil.consumerProperties, null);

        tenv.execute("DwdOrderInfoAppJob");

    }
}
