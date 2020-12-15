package com.databoy.udf;

import com.databoy.beans.OrderInfo;
import com.databoy.beans.OrderInfoDetail;
import com.databoy.utils.PhoenixUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
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
public class OrderInfoJoinMapFunction extends RichMapFunction<OrderInfo, OrderInfoDetail> {

    private Connection connection;
    private PreparedStatement customerStatement;
    private PreparedStatement adStatement;

    @Override
    public void open(Configuration conf) throws Exception {
        connection = PhoenixUtil.createConnection();
        customerStatement = connection.prepareStatement("select \"name\",\"city\" from \"customer\" where \"customer_id\"=?");
        adStatement = connection.prepareStatement("select \"name\",\"type\",\"cpc\",\"cpm\",\"pay_mode\",\"adlist_id\" from \"ad\" where \"ad_id\"=?");
    }

    @Override
    public void close() throws Exception {

        PhoenixUtil.closeResource(customerStatement,null,null);
        PhoenixUtil.closeResource(adStatement,null,connection);
    }

    @Override
    public OrderInfoDetail map(OrderInfo orderInfo) throws Exception {

        OrderInfoDetail orderInfoDetail = new OrderInfoDetail();
        orderInfoDetail.setOrderId(orderInfo.getOrderId());
        orderInfoDetail.setCustomerId(orderInfo.getCustomerId());
        orderInfoDetail.setAdId(orderInfo.getAdId());
        orderInfoDetail.setMediaId(orderInfo.getMediaId());
        orderInfoDetail.setFlowId(orderInfo.getFlowId());
        orderInfoDetail.setCount(orderInfo.getCount());
        orderInfoDetail.setCreateTime(orderInfo.getCreateTime());


        // customer连接
        customerStatement.setString(1,orderInfo.getCustomerId());
        ResultSet customerResultSet = customerStatement.executeQuery();
        if(customerResultSet.next()){
            String customerName = customerResultSet.getString("name");
            String customerCity = customerResultSet.getString("city");

            orderInfoDetail.setCustomerName(customerName);
            orderInfoDetail.setCustomerCity(customerCity);

            PhoenixUtil.closeResource(null,customerResultSet,null);
        }

        // ad连接
        adStatement.setString(1,orderInfo.getAdId());
        ResultSet adResultSet = adStatement.executeQuery();
        if(adResultSet.next()){

            String adName = adResultSet.getString("name");
            String adType = adResultSet.getString("type");
            double cpc = adResultSet.getDouble("cpc");
            double cpm = adResultSet.getDouble("cpm");
            String payMode = adResultSet.getString("pay_mode");
            String adlistId = adResultSet.getString("adlist_id");

            orderInfoDetail.setAdName(adName);
            orderInfoDetail.setAdType(adType);
            orderInfoDetail.setCpc(cpc);
            orderInfoDetail.setCpm(cpm);
            orderInfoDetail.setPayMode(payMode);
            orderInfoDetail.setAdlistId(adlistId);

            PhoenixUtil.closeResource(null,adResultSet,null);
        }


        return orderInfoDetail;
    }
}
