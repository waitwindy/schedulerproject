package com.ultrapower.scheduler.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: xlt
 * @Description:
 * @Date: Created in 11:20 2018/8/10
 */
public class CoreConfig {

    private static final String PROPERTIES = "application-core.properties";
    private static final Log log = LogFactory.getLog(CoreConfig.class);


    public static  Boolean DO_INTERVAL;
    public static  String RAW_TABLE_NAME  ;
    public static String ES_CLUSTERNAME;
    public static String ES_IPS;
    public static String ES_PORTS;
    public static String BUSLOG_INDEXNAME;
    public static Integer INDEX_INTERNAL;
    public static String INDEX_BUSTYPE_COLUME;
    public static String INDEX_STARTTIME_SUFFIX;
    public static String INDEX_STATUS_SUFFIX;
    public static String INDEX_ISTIMEOUT_SUFFIX;
    public static String INDEX_COSTTIME_SUFFIX;
    public static Integer JOB_CONVERGE_TIMES;
    public static long BASELINE_TIME;
    public static String BUSTYPE_TIME_COLUME;
    public static String BUSTYPE_STATUS_COLUME;
    public static String BUSTYPE_COSTTIME_COLUME;
    public static String BUSTYPE_ISOUTTIME_COLUME;
    public static int R_HOUR_CONVERGE_TIMES;
    public static String INDEX_RAW_HOUR_NAME;
    public static String INDEX_RAW_MONTH_NAME;
    public static String INDEX_RAW_DAY_NAME;
    public static String LOG4J_PATH;

    static {
        Properties pro = new Properties();
        try {
            InputStream in = CoreConfig.class.getClassLoader().getResourceAsStream(PROPERTIES);
            pro.load(in);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("read the " + PROPERTIES + " error !!!", e);
        }

        JOB_CONVERGE_TIMES= Integer.valueOf(pro.getProperty("job.converge.times"));

        RAW_TABLE_NAME=pro.getProperty("raw.table.name");
        DO_INTERVAL= Boolean.valueOf(pro.getProperty("do.interval"));
        ES_CLUSTERNAME=pro.getProperty("es.clustername");
        ES_IPS=pro.getProperty("es.ips");
        ES_PORTS=pro.getProperty("es.ports");
        BUSLOG_INDEXNAME=pro.getProperty("buslog.indexname");
        INDEX_INTERNAL= Integer.valueOf(pro.getProperty("index.internal"));
        INDEX_BUSTYPE_COLUME=pro.getProperty("index.bustype.colume");
        INDEX_STARTTIME_SUFFIX=pro.getProperty("inde.starttime.suffix");
        INDEX_STATUS_SUFFIX=pro.getProperty("index.status.suffix");
        INDEX_ISTIMEOUT_SUFFIX=pro.getProperty("index.istimeout.suffix");
        INDEX_COSTTIME_SUFFIX=pro.getProperty("index.costtime.suffix");

        BASELINE_TIME= Long.parseLong(pro.getProperty("baseLine.time"));
        BUSTYPE_TIME_COLUME=pro.getProperty("bustype.time.colume");
        BUSTYPE_STATUS_COLUME=pro.getProperty("bustype.status.colume");
        BUSTYPE_COSTTIME_COLUME=pro.getProperty("bustype.costtime.colume");

        BUSTYPE_ISOUTTIME_COLUME=pro.getProperty("bustype.isouttime.colume");
        R_HOUR_CONVERGE_TIMES= Integer.parseInt(pro.getProperty("r.hour.converge.times"));
        INDEX_RAW_HOUR_NAME=pro.getProperty("index.raw.hour.name");
        INDEX_RAW_MONTH_NAME=pro.getProperty("index.raw.month.name");
        INDEX_RAW_DAY_NAME=pro.getProperty("index.raw.day.name");

        LOG4J_PATH=pro.getProperty("log4j.path");
    }
}
