package com.databoy.udf;

import com.databoy.beans.DbDataBean;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.udf
 * @Description: TODO
 * @date 2020/12/13 21:12
 */
public class TableFilterFunction  implements FilterFunction<DbDataBean> {

    private String table;

    public TableFilterFunction() {
    }

    public TableFilterFunction(String table) {
        this.table = table;
    }

    @Override
    public boolean filter(DbDataBean dbDataBean) throws Exception {

        return table.equals(dbDataBean.getTable());
    }
}
