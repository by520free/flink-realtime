package com.databoy.beans;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.beans
 * @Description: TODO
 * @date 2020/12/13 22:28
 */
public class AdBean {

    private int id;
    private String adId;
    private String name;
    private String type;
    private String title;
    private String document;
    private double cpc;
    private double cpm;
    private String payMode;
    private String adlistId;
    private int customerId;

    /**
     *  CREATE TABLE IF NOT EXISTS "ad"(
     *      "ad_id" VARCHAR NOT NULL PRIMARY KEY,
     *      "id" INTEGER,
     *      "name" VARCHAR,
     *      "type" VARCHAR,
     *      "title" VARCHAR,
     *      "document" VARCHAR,
     *      "cpc" DOUBLE,
     *      "cpm" DOUBLE,
     *      "pay_mode" VARCHAR,
     *      "adlist_id" VARCHAR,
     *      "customer_id" VARCHAR
     *  ) SALT_BUCKETS = 3;
     */

    public AdBean() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDocument() {
        return document;
    }

    public void setDocument(String document) {
        this.document = document;
    }

    public double getCpc() {
        return cpc;
    }

    public void setCpc(double cpc) {
        this.cpc = cpc;
    }

    public double getCpm() {
        return cpm;
    }

    public void setCpm(double cpm) {
        this.cpm = cpm;
    }

    public String getPayMode() {
        return payMode;
    }

    public void setPayMode(String payMode) {
        this.payMode = payMode;
    }

    public String getAdlistId() {
        return adlistId;
    }

    public void setAdlistId(String adlistId) {
        this.adlistId = adlistId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }
}
