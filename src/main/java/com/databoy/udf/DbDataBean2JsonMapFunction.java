package com.databoy.udf;

import com.databoy.beans.DbDataBean;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.udf
 * @Description: TODO
 * @date 2020/12/13 21:22
 */
public class DbDataBean2JsonMapFunction implements MapFunction<DbDataBean,String> {
    @Override
    public String map(DbDataBean dbDataBean) throws Exception {

        return dbDataBean.getData().toString();
    }
}
