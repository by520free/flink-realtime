package com.databoy.udf;

import cn.hutool.json.JSONUtil;
import com.databoy.beans.*;
import com.databoy.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.udf
 * @Description: TODO
 * @date 2020/12/13 22:14
 */
public class PhoenixSinkFunction extends RichSinkFunction<String> {

    private String table;
    private Connection connection;
    private PreparedStatement prepareStatement;


    public PhoenixSinkFunction() {
    }

    public PhoenixSinkFunction(String table) {
        this.table = table;
    }


    @Override
    public void open(Configuration conf) throws Exception {

        this.connection = PhoenixUtil.createConnection();
    }

    @Override
    public void close() throws Exception {

        PhoenixUtil.closeResource(prepareStatement,null,connection);
    }

    @Override
    public void invoke(String value) throws Exception {

        switch (table){
            case "ad":
                processAd(JSONUtil.toBean(value,AdBean.class));
                break;
            case "media":
                processMedia(JSONUtil.toBean(value,Media.class));
                break;
            case "flow":
                processFlow(JSONUtil.toBean(value,Flow.class));
                break;
            case "customer":
                processCustomer(JSONUtil.toBean(value,Customer.class));
                break;
            default:
                processUser(JSONUtil.toBean(value,User.class));
                break;
        }
    }

    private void processMedia(Media media) throws SQLException {

        if(prepareStatement == null){

            String baseSql = "UPSERT INTO \"media\" VALUES(?,?,?,?,?)";
            prepareStatement = connection.prepareStatement(baseSql);
        }

        prepareStatement.setString(1,media.getMediaId());
        prepareStatement.setInt(2,media.getId());
        prepareStatement.setString(3,media.getAttribute());
        prepareStatement.setString(4,media.getCreateTime());
        prepareStatement.setString(5,media.getUpdateTime());

        prepareStatement.execute();
        connection.commit();
    }

    private void processFlow(Flow flow) throws SQLException {

        if(prepareStatement == null){

            String baseSql = "UPSERT INTO \"flow\" VALUES(?,?,?)";
            prepareStatement = connection.prepareStatement(baseSql);
        }

        prepareStatement.setString(1,flow.getFlowId());
        prepareStatement.setInt(2,flow.getId());
        prepareStatement.setString(3,flow.getCreateTime());

        prepareStatement.execute();
        connection.commit();
    }

    private void processCustomer(Customer customer) throws SQLException {

        if(prepareStatement == null){

            String baseSql = "UPSERT INTO \"customer\" VALUES(?,?,?,?,?)";
            prepareStatement = connection.prepareStatement(baseSql);
        }

        prepareStatement.setString(1,customer.getCustomerId());
        prepareStatement.setInt(2,customer.getId());
        prepareStatement.setString(3,customer.getName());
        prepareStatement.setString(4,customer.getCity());
        prepareStatement.setString(5,customer.getDesc());

        prepareStatement.execute();
        connection.commit();
    }

    private void processAd(AdBean adBean) throws SQLException {

        if(prepareStatement == null){

            String baseSql = "UPSERT INTO  \"ad\" VALUES(?,?,?,?,?,?,?,?,?,?,?)";
            prepareStatement = connection.prepareStatement(baseSql);
        }

        prepareStatement.setString(1,adBean.getAdId());
        prepareStatement.setInt(2,adBean.getId());
        prepareStatement.setString(3,adBean.getName());
        prepareStatement.setString(4,adBean.getType());
        prepareStatement.setString(5,adBean.getTitle());
        prepareStatement.setString(6,adBean.getDocument());
        prepareStatement.setDouble(7,adBean.getCpc());
        prepareStatement.setDouble(8,adBean.getCpm());
        prepareStatement.setString(9,adBean.getPayMode());
        prepareStatement.setString(10,adBean.getAdlistId());
        prepareStatement.setString(11,adBean.getCustomerId() + "");

        prepareStatement.execute();
        connection.commit();
    }

    private void processUser(User user) throws SQLException {

        if(prepareStatement == null){

            String baseSql = "UPSERT INTO \"user\" VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            prepareStatement = connection.prepareStatement(baseSql);
        }

        prepareStatement.setString(1,user.getUid());
        prepareStatement.setInt(2,user.getId());
        prepareStatement.setString(3,user.getOs());
        prepareStatement.setString(4,user.getCity());
        prepareStatement.setString(5,user.getGender());
        prepareStatement.setInt(6,user.getAge());
        prepareStatement.setString(7,user.getEducation());
        prepareStatement.setString(8,user.getOccupation());
        prepareStatement.setInt(9,user.getMarriage());
        prepareStatement.setString(10,user.getHobby());
        prepareStatement.setString(11,user.getIncome());
        prepareStatement.setString(12,user.getConsume());
        prepareStatement.setInt(13,user.getHasCar());
        prepareStatement.setInt(14,user.getHasHouse());
        prepareStatement.setInt(15,user.getHasChild());

        prepareStatement.execute();
        connection.commit();
    }

}
