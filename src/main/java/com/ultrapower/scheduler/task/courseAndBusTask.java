package com.ultrapower.scheduler.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ultrapower.scheduler.job.convergeDayDataJob;
import com.ultrapower.scheduler.model.CoreConfig;
import com.ultrapower.scheduler.util.ElasticSearchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: xlt
 * @Description:汇聚环节和业务的任务。
 * @Date: Created in 11:31 2018/8/10
 */
public class courseAndBusTask implements Runnable {

    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(courseAndBusTask.class);

    private String indexName;
    private Long startTime;
    private Long endTime;

    public courseAndBusTask(String indexName, Long startTime, Long endTime) {
        this.indexName = indexName;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public void run() {
//      获取索引的mapping信息。筛选出所有的环节名称。
        List<String> allCourse = null;

        try {
            allCourse = ElasticSearchHandler.getAllCourse(indexName);
        } catch (Exception e) {
            System.out.println("获取环节失败！！！！");
            e.printStackTrace();
        }
        String busTypeColume = CoreConfig.INDEX_BUSTYPE_COLUME;
        List<JSONObject> jsonObjects = new ArrayList<>();

        log.info("==================开始判断索引是否为空================");
        if ( ElasticSearchHandler.ifIndexExits(indexName) ) {

            if (allCourse.size() != 0 && allCourse != null) {
                for (String course : allCourse) {

                    String startTimeColum = course + CoreConfig.INDEX_STARTTIME_SUFFIX;
                    String statusColume = course + CoreConfig.INDEX_STATUS_SUFFIX;
                    String isOutTimeColume = course + CoreConfig.INDEX_ISTIMEOUT_SUFFIX;
                    String costTimeColume = course + CoreConfig.INDEX_COSTTIME_SUFFIX;

                    List<JSONObject> courseValue = ElasticSearchHandler.getCourseNum2(
                            indexName,
                            course,
                            busTypeColume,
                            startTimeColum,
                            startTime,
                            endTime,
                            statusColume,
                            isOutTimeColume,
                            costTimeColume);
                    jsonObjects.addAll(courseValue);

                }

            } else {
                log.info("环节类型为空");
            }
//        统计业务的成功失败 超时量
//        1.查询业务的开始时间字段 查询业务的成功失败业务状态字段.
            List<JSONObject> busTypeMsg = ElasticSearchHandler.getCourseNum2(indexName, "all", busTypeColume,
                    CoreConfig.BUSTYPE_TIME_COLUME,
                    startTime,
                    endTime,
                    CoreConfig.BUSTYPE_STATUS_COLUME,
                    CoreConfig.BUSTYPE_ISOUTTIME_COLUME,
                    CoreConfig.BUSTYPE_COSTTIME_COLUME);
            jsonObjects.addAll(busTypeMsg);
        } else {
            log.info(indexName+"索引不存在");
        }

        if (jsonObjects.size() != 0 && jsonObjects != null) {
            ElasticSearchHandler.createIndexBulkResponse(CoreConfig.RAW_TABLE_NAME, "data", jsonObjects);
        }

    }

    public static void main(String[] args) {

        String indexName = "buslog_20180430_20180506";
//        获取索引的mapping信息。筛选出所有的环节名称。
        List<String> allCourse = null;
        try {
            allCourse = ElasticSearchHandler.getAllCourse(indexName);
            System.out.println(allCourse);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String busTypeColume = "web_menuname.keyword";

        Long startTime = 1525240203711L;
        Long endTime = 1525270203711L;

        List<JSONObject> jsonObjects = new ArrayList<>();

        if (allCourse.size() != 0 && allCourse != null) {

            for (String course : allCourse) {

                String startTimeColum = course + "_starttime";
                String statusColume = course + "_success";
                String isOutTimeColume = course + "_isoutTime";
                String costTimeColume = course + "_costtime";
                List<JSONObject> courseValue = ElasticSearchHandler.getCourseNum2(
                        indexName,
                        course,
                        busTypeColume,
                        startTimeColum,
                        startTime,
                        endTime,
                        statusColume,
                        isOutTimeColume,
                        costTimeColume);
                jsonObjects.addAll(courseValue);
            }

        } else {
            System.out.println("=======环节类型为空");
        }
        System.out.println(jsonObjects);
        ElasticSearchHandler.createIndexBulkResponse("alldata_raw", "data", jsonObjects);

    }

}
