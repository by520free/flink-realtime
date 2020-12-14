package com.databoy.beans;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.beans
 * @Description: TODO
 * @date 2020/12/13 22:28
 */
public class Flow {

    private int id;
    private String flowId;
    private int mediaId;
    private String createTime;


    /**
     *  CREATE TABLE IF NOT EXISTS "flow"(
     *      "flow_id" VARCHAR NOT NULL PRIMARY KEY,
     *      "id" INTEGER,
     *      "create_time" VARCHAR
     *  ) SALT_BUCKETS = 3
     */

    public Flow() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    public int getMediaId() {
        return mediaId;
    }

    public void setMediaId(int mediaId) {
        this.mediaId = mediaId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
