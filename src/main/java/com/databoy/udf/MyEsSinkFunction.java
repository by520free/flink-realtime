package com.databoy.udf;

import cn.hutool.json.JSONUtil;
import com.databoy.beans.OrderInfoDetail;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/15
 * @since 1.0.0
 */
public class MyEsSinkFunction implements ElasticsearchSinkFunction<String>{

    public IndexRequest createIndexRequest(String element) {
        Map<String, Object> json = new HashMap<>();
        OrderInfoDetail orderInfoDetail = JSONUtil.toBean(element, OrderInfoDetail.class);
        json.put("order_id", orderInfoDetail.getOrderId());
        json.put("customer_id", orderInfoDetail.getCustomerId());
        json.put("customer_name",orderInfoDetail.getCustomerName());
        json.put("customer_city",orderInfoDetail.getCustomerCity());
        json.put("ad_id",orderInfoDetail.getAdId());
        json.put("ad_name",orderInfoDetail.getAdName());
        json.put("ad_type",orderInfoDetail.getAdType());
        json.put("cpc",orderInfoDetail.getCpc());
        json.put("cpm",orderInfoDetail.getCpm());
        json.put("pay_mode",orderInfoDetail.getPayMode());
        json.put("adlist_id",orderInfoDetail.getAdlistId());
        json.put("media_id",orderInfoDetail.getMediaId());
        json.put("flow_id",orderInfoDetail.getFlowId());
        json.put("count",orderInfoDetail.getCount());
        json.put("create_time",orderInfoDetail.getCreateTime());


        return Requests.indexRequest()
                .index("first-order")
                .type("_doc")
                .id(orderInfoDetail.getOrderId())
                .source(json);
    }

    @Override
    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }
}
