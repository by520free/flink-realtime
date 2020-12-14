package com.databoy.beans;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.beans
 * @Description: TODO
 * @date 2020/12/13 22:29
 */
public class Customer {

    private int id;
    private String customerId;
    private String name;
    private String city;
    private String desc;

    /**
     * CREATE TABLE IF NOT EXISTS "customer"(
     *  "customer_id" VARCHAR NOT NULL PRIMARY KEY,
     *  "id" INTEGER,
     *  "name" VARCHAR,
     *  "city" VARCHAR,
     *  "desc" VARCHAR
     * ) SALT_BUCKETS = 3
     */


    public Customer() {
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
