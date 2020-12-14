package com.databoy.beans;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.beans
 * @Description: TODO
 * @date 2020/12/13 22:28
 */
public class Media {

    private int id;
    private String mediaId;
    private String attribute;
    private String createTime;
    private String updateTime;

    /**
     * CREATE TABLE IF NOT EXISTS "meida"(
     *    "media_id" VARCHAR NOT NULL PRIMARY KEY,
     *    "id" INTEGER NOT NULL,
     *    "attribute" VARCHAR NOT NULL,
     *    "create_time" VARCHAR NOT NULL,
     *    "update_time" VARCHAR NOT NULL
     * ) SALT_BUCKETS = 3
     */

    public Media() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMediaId() {
        return mediaId;
    }

    public void setMediaId(String mediaId) {
        this.mediaId = mediaId;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
