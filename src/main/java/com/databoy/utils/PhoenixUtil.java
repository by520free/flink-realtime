package com.databoy.utils;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.utils
 * @Description: TODO
 * @date 2020/12/13 22:02
 */
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.mail.MailUtil;
import cn.hutool.setting.dialect.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.phoenix.jdbc.PhoenixDriver;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/13
 * @since 1.0.0
 */
public class PhoenixUtil {

    private static Logger logger = LoggerFactory.getLogger(PhoenixUtil.class);
    private static Properties props = null;
    private static long counter = 1L;
    private static Properties prop = Props.getProp("setting.properties");

    static {
        props = new Properties();
        props.setProperty("phoenix.query.timeoutMs", "60000000");
        props.setProperty("phoenix.query.threadPoolSize", "128");
        props.setProperty("phoenix.query.queueSize", "5000");
        props.setProperty("hbase.client.scanner.timeout.period", "300000");
        /*props.setProperty("phoenix.coprocessor.maxServerCacheTimeToLiveMs", "1800000");
        props.setProperty("phoenix.coprocessor.maxMetaDataCacheTimeToLiveMs", "1800000");
        props.setProperty("phoenix.coprocessor.maxMetaDataCacheSize", "52428800");
        props.setProperty("hbase.regionserver.handler.count", "100");
        props.setProperty("hbase.hstore.flusher.count", "10");
        props.setProperty("hbase.hregion.memstore.block.multiplier", "8");
        props.setProperty("hbase.hlog.asyncer.number", "100");
        props.setProperty("hbase.hstore.blockingStoreFiles", "1000");
        props.setProperty("org.apache.phoenix.regionserver.index.handler.count", "200");*/
        try {
            Class.forName(PhoenixDriver.class.getName());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException("Failed loading Phoenix JDBC driver", e);
        }
    }

    /**
     * 创建实例
     *
     * @return
     */
    public static Connection createConnection() {

        Connection conn = null;

        try {
            if (props != null) {
                if(conn == null) {
                    conn = DriverManager.getConnection(prop.getProperty("phoenix.jdbc.url"), props);
                }
            } else {
                if(conn == null) {
                    conn = DriverManager.getConnection(prop.getProperty("phoenix.jdbc.url"));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        return conn;

    }




    private static void closeResultSet(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException se) {
            logger.error(se.getMessage(), se);
            try {
                MailUtil.send("2470898917@qq.com","项目异常","PhoenixUtil类closeResultSet方法异常内容:" +se.getMessage(),false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void closeStatement(Statement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException se) {
            logger.error(se.getMessage(), se);
            try {
                MailUtil.send("2470898917@qq.com","项目异常","PhoenixUtil类closeStatement方法异常内容:" +se.getMessage(),false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException se) {
            logger.error(se.getMessage(), se);
            try {
                MailUtil.send("2470898917@qq.com","项目异常","PhoenixUtil类closeConnection方法异常内容:" +se.getMessage(),false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeResource(Statement sm, ResultSet rs,Connection conn) {
        closeResultSet(rs);
        closeStatement(sm);
        closeConnection(conn);
    }

    private static void rollBack(Connection conn) {
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException se) {
            logger.error(se.getMessage(), se);
        }
    }

    public static void save2Hbase(){


    }

    public static void main(String[] args) {

        System.out.println(createConnection());
    }
}
