package com.databoy.udf;

import com.databoy.beans.OrderInfoDetail;
import com.databoy.utils.PhoenixUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/15
 * @since 1.0.0
 */
public class FirstOrderFilterFunction extends RichFilterFunction<OrderInfoDetail> {

    private Connection connection;
    private PreparedStatement orderStatusStatement;
    private PreparedStatement orderStatusInsertStatement;

    @Override
    public void open(Configuration conf) throws Exception {

        connection = PhoenixUtil.createConnection();
        orderStatusStatement = connection.prepareStatement("select \"statu\" from \"order_statu\" where \"customer_id\"=?");
        orderStatusInsertStatement = connection.prepareStatement("UPSERT INTO \"order_statu\" VALUES(?,?)");
    }

    @Override
    public void close() throws Exception {

        PhoenixUtil.closeResource(orderStatusStatement,null,connection);
    }

    @Override
    public boolean filter(OrderInfoDetail orderInfoDetail) throws Exception {

        boolean flag = false;

        orderStatusStatement.setString(1,orderInfoDetail.getCustomerId());
        ResultSet resultSet = orderStatusStatement.executeQuery();

        if(!resultSet.next()){

            orderStatusInsertStatement.setString(1,orderInfoDetail.getCustomerId());
            orderStatusInsertStatement.setBoolean(2,true);
            orderStatusInsertStatement.execute();
            connection.commit();

            flag = true;
        }

        return flag;
    }
}
