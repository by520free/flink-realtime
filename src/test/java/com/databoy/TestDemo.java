package com.databoy;

import cn.hutool.json.JSONUtil;
import com.databoy.beans.Customer;
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
}
