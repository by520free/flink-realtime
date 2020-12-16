package com.databoy.udf;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.databoy.beans.*;
import com.databoy.utils.PhoenixUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/16
 * @since 1.0.0
 */
public class UserLogJoinMapFunction extends RichMapFunction<String,String> {

    private Connection connection;
    private PreparedStatement adStatement;
    private PreparedStatement userStatement;
    private PreparedStatement customerStatement;

    @Override
    public void open(Configuration parameters) throws Exception {

        connection = PhoenixUtil.createConnection();
        adStatement = connection.prepareStatement("SELECT \"name\",\"type\",\"title\",\"document\",\"cpc\",\"cpm\",\"pay_mode\",\"adlist_id\",\"customer_id\" from \"ad\" where \"ad_id\"=?");
        userStatement = connection.prepareStatement("SELECT \"os\",\"city\",\"gender\",\"age\",\"education\",\"occupation\",\"marriage\",\"hobby\",\"income\",\"consume\",\"hasCar\",\"hasHouse\",\"hasChild\" from \"user\" where \"uid\"=?");
        customerStatement = connection.prepareStatement("SELECT \"name\" from \"customer\" where \"customer_id\"=?");
    }

    @Override
    public void close() throws Exception {

        PhoenixUtil.closeResource(adStatement,null,null);
        PhoenixUtil.closeResource(userStatement,null,null);
        PhoenixUtil.closeResource(adStatement,null,connection);
    }

    @Override
    public String map(String s) throws Exception {

        JSONObject logJson = JSONUtil.parseObj(s);
        String logType = logJson.getStr("type");

        switch (logType){
            case "pv":
                PvBean pvBean = JSONUtil.toBean(s, PvBean.class);
                return processPvLog(pvBean);
            case "show":
                ShowBean showBean = JSONUtil.toBean(s, ShowBean.class);
                return processShowLog(showBean);
            case "click":
                ClickBean clickBean = JSONUtil.toBean(s, ClickBean.class);
                return processClickLog(clickBean);
            default:
                SubmitBean submitBean = JSONUtil.toBean(s, SubmitBean.class);
                return processSubmitLog(submitBean);
        }
    }



    public  String processPvLog(PvBean pvBean) throws SQLException {

        userStatement.setString(1,pvBean.getUid());
        ResultSet userResult = userStatement.executeQuery();

        PvDetail pvDetail = new PvDetail();
        pvDetail.setCity(pvBean.getCity());
        pvDetail.setViewId(pvBean.getViewId());
        pvDetail.setCreateTime(pvBean.getCreateTime());
        pvDetail.setUid(pvBean.getUid());
        if(userResult.next()){

            pvDetail.setUserOs(userResult.getString("os"));
            pvDetail.setUserAge(userResult.getInt("age"));
            pvDetail.setUserEducation(userResult.getString("education"));
            pvDetail.setUserOs(userResult.getString("occupation"));
            pvDetail.setUserMarriage(userResult.getInt("marriage"));
            pvDetail.setUserHobby(userResult.getString("hobby"));
            pvDetail.setUserIncome(userResult.getString("income"));
            pvDetail.setUserConsume(userResult.getString("consume"));
            pvDetail.setHasCar(userResult.getInt("hasCar"));
            pvDetail.setHasHouse(userResult.getInt("hasHouse"));
            pvDetail.setHasChild(userResult.getInt("hasChild"));
        }
        pvDetail.setOperId(pvBean.getOperId());
        pvDetail.setFlowId(pvBean.getFlowId());
        pvDetail.setType(pvBean.getType());
        pvDetail.setIp(pvBean.getIp());
        pvDetail.setSex(pvBean.getSex());
        pvDetail.setBrowserModel(pvBean.getBrowserModel());
        pvDetail.setPlatformId(pvBean.getPlatformId());
        pvDetail.setMediaId(pvBean.getMediaId());

        PhoenixUtil.closeResource(null,userResult,null);

        return JSONUtil.toJsonStr(pvDetail);
    }

    private String processShowLog(ShowBean showBean) throws SQLException {

        userStatement.setString(1,showBean.getUid());
        ResultSet userResult = userStatement.executeQuery();
        adStatement.setString(1,showBean.getAdId());
        ResultSet adResult = adStatement.executeQuery();
        customerStatement.setString(1,showBean.getCustomerId());
        ResultSet customerResult = customerStatement.executeQuery();

        ShowDetail showDetail = new ShowDetail();
        showDetail.setCity(showBean.getCity());
        showDetail.setAdId(showBean.getAdId());
        if(adResult.next()){

            showDetail.setAdName(adResult.getString("name"));
            showDetail.setAdType(adResult.getString("type"));
            showDetail.setAdTitle(adResult.getString("title"));
            showDetail.setAdDocument(adResult.getString("document"));
            showDetail.setAdCpc(adResult.getDouble("cpc"));
            showDetail.setAdCpm(adResult.getDouble("cpm"));
            showDetail.setAdPayMode(adResult.getString("pay_mode"));
        }
        showDetail.setCreateTime(showBean.getCreateTime());
        showDetail.setUid(showBean.getUid());
        if(userResult.next()){

            showDetail.setUserOs(userResult.getString("os"));
            showDetail.setUserAge(userResult.getInt("age"));
            showDetail.setUserEducation(userResult.getString("education"));
            showDetail.setUserOs(userResult.getString("occupation"));
            showDetail.setUserMarriage(userResult.getInt("marriage"));
            showDetail.setUserHobby(userResult.getString("hobby"));
            showDetail.setUserIncome(userResult.getString("income"));
            showDetail.setUserConsume(userResult.getString("consume"));
            showDetail.setHasCar(userResult.getInt("hasCar"));
            showDetail.setHasHouse(userResult.getInt("hasHouse"));
            showDetail.setHasChild(userResult.getInt("hasChild"));
        }
        showDetail.setOperId(showBean.getOperId());
        showDetail.setMediaId(showBean.getMediaId());
        showDetail.setFlowId(showBean.getFlowId());
        showDetail.setPlatformId(showBean.getPlatformId());
        showDetail.setType(showBean.getType());
        showDetail.setAdlistId(showBean.getAdlistId());
        showDetail.setCustomerId(showBean.getCustomerId());
        if(customerResult.next()){

            showDetail.setCustomerName(customerResult.getString("name"));
        }
        showDetail.setCost(showBean.getCost());

        PhoenixUtil.closeResource(null,adResult,null);
        PhoenixUtil.closeResource(null,userResult,null);
        PhoenixUtil.closeResource(null,customerResult,null);


        return JSONUtil.toJsonStr(showDetail);
    }

    private String processClickLog(ClickBean clickBean) throws SQLException {

        userStatement.setString(1,clickBean.getUid());
        ResultSet userResult = userStatement.executeQuery();
        adStatement.setString(1,clickBean.getAdId());
        ResultSet adResult = adStatement.executeQuery();
        customerStatement.setString(1,clickBean.getCustomerId());
        ResultSet customerResult = customerStatement.executeQuery();

        ClickDetail clickDetail = new ClickDetail();
        clickDetail.setCity(clickBean.getCity());
        clickDetail.setAdId(clickBean.getAdId());
        if(adResult.next()){

            clickDetail.setAdName(adResult.getString("name"));
            clickDetail.setAdType(adResult.getString("type"));
            clickDetail.setAdTitle(adResult.getString("title"));
            clickDetail.setAdDocument(adResult.getString("document"));
            clickDetail.setAdCpc(adResult.getDouble("cpc"));
            clickDetail.setAdCpm(adResult.getDouble("cpm"));
            clickDetail.setAdPayMode(adResult.getString("pay_mode"));
        }
        clickDetail.setCreateTime(clickBean.getCreateTime());
        clickDetail.setUid(clickBean.getUid());
        if(userResult.next()){

            clickDetail.setUserOs(userResult.getString("os"));
            clickDetail.setUserAge(userResult.getInt("age"));
            clickDetail.setUserEducation(userResult.getString("education"));
            clickDetail.setUserOs(userResult.getString("occupation"));
            clickDetail.setUserMarriage(userResult.getInt("marriage"));
            clickDetail.setUserHobby(userResult.getString("hobby"));
            clickDetail.setUserIncome(userResult.getString("income"));
            clickDetail.setUserConsume(userResult.getString("consume"));
            clickDetail.setHasCar(userResult.getInt("hasCar"));
            clickDetail.setHasHouse(userResult.getInt("hasHouse"));
            clickDetail.setHasChild(userResult.getInt("hasChild"));
        }
        clickDetail.setOperId(clickBean.getOperId());
        clickDetail.setPlatformId(clickBean.getPlatformId());
        clickDetail.setMediaId(clickBean.getMediaId());
        clickDetail.setFlowId(clickBean.getFlowId());
        clickDetail.setAdlistId(clickBean.getAdlistId());
        clickDetail.setCustomerId(clickBean.getCustomerId());
        if(customerResult.next()){

            clickDetail.setCustomerName(customerResult.getString("name"));
        }
        clickDetail.setType(clickBean.getType());
        clickDetail.setCost(clickBean.getCost());


        PhoenixUtil.closeResource(null,adResult,null);
        PhoenixUtil.closeResource(null,userResult,null);
        PhoenixUtil.closeResource(null,customerResult,null);

        return JSONUtil.toJsonStr(clickDetail);
    }

    private String processSubmitLog(SubmitBean submitBean) throws SQLException {


        userStatement.setString(1,submitBean.getUid());
        ResultSet userResult = userStatement.executeQuery();
        adStatement.setString(1,submitBean.getAdId());
        ResultSet adResult = adStatement.executeQuery();
        customerStatement.setString(1,submitBean.getCustomerId());
        ResultSet customerResult = customerStatement.executeQuery();

        SubmitDetail submitDetail = new SubmitDetail();
        submitDetail.setAdId(submitBean.getAdId());
        if(adResult.next()){

            submitDetail.setAdName(adResult.getString("name"));
            submitDetail.setAdType(adResult.getString("type"));
            submitDetail.setAdTitle(adResult.getString("title"));
            submitDetail.setAdDocument(adResult.getString("document"));
            submitDetail.setAdCpc(adResult.getDouble("cpc"));
            submitDetail.setAdCpm(adResult.getDouble("cpm"));
            submitDetail.setAdPayMode(adResult.getString("pay_mode"));
        }
        submitDetail.setCustomerId(submitBean.getCustomerId());
        if(customerResult.next()){

            submitDetail.setCustomerName(customerResult.getString("name"));
        }
        submitDetail.setAdlistId(submitBean.getAdlistId());
        submitDetail.setUid(submitBean.getUid());
        if(userResult.next()){

            submitDetail.setCity(userResult.getString("city"));
            submitDetail.setUserOs(userResult.getString("os"));
            submitDetail.setUserAge(userResult.getInt("age"));
            submitDetail.setUserEducation(userResult.getString("education"));
            submitDetail.setUserOs(userResult.getString("occupation"));
            submitDetail.setUserMarriage(userResult.getInt("marriage"));
            submitDetail.setUserHobby(userResult.getString("hobby"));
            submitDetail.setUserIncome(userResult.getString("income"));
            submitDetail.setUserConsume(userResult.getString("consume"));
            submitDetail.setHasCar(userResult.getInt("hasCar"));
            submitDetail.setHasHouse(userResult.getInt("hasHouse"));
            submitDetail.setHasChild(userResult.getInt("hasChild"));
        }
        submitDetail.setOperId(submitBean.getOperId());
        submitDetail.setType(submitBean.getType());
        submitDetail.setCreatTime(submitBean.getCreatTime());
        submitDetail.setPlatformId(submitBean.getPlatformId());
        submitDetail.setMediaId(submitBean.getMediaId());
        submitDetail.setFlowId(submitBean.getFlowId());
        submitDetail.setCost(submitBean.getCost());


        PhoenixUtil.closeResource(null,adResult,null);
        PhoenixUtil.closeResource(null,userResult,null);
        PhoenixUtil.closeResource(null,customerResult,null);

        return JSONUtil.toJsonStr(submitDetail);

    }
}
