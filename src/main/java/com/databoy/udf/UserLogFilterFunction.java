package com.databoy.udf;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/16
 * @since 1.0.0
 */
public class UserLogFilterFunction implements FilterFunction<String> {

    private String type;

    public UserLogFilterFunction() {
    }

    public UserLogFilterFunction(String type) {
        this.type = type;
    }


    @Override
    public boolean filter(String s) throws Exception {

        JSONObject userLogJson = JSONUtil.parseObj(s);

        return type.equals(userLogJson.getStr("type"));
    }
}
