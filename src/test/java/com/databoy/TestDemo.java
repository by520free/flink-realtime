package com.databoy;

import cn.hutool.json.JSONUtil;
import com.databoy.beans.Customer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

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

        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);


        String sql = "CREATE TABLE student (\n" +
                "  `user_id` STRING,\n" +
                "  `name` STRING,\n" +
                "  `age` INT,\n" +
                "  `gender` STRING,\n" +
                "  `ts` TIMESTAMP\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test',\n" +
                "  'properties.bootstrap.servers' = 'master01:9092,slave01:9092,slave02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")\n";

        tblEnv.executeSql(sql);

        TableResult result = tblEnv.executeSql("select * from student");

        result.print();

        tblEnv.execute("tableDemo");
    }
}
