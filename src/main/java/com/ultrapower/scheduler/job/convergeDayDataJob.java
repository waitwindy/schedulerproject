package com.ultrapower.scheduler.job;

import com.alibaba.fastjson.JSONObject;
import com.ultrapower.scheduler.model.CoreConfig;
import com.ultrapower.scheduler.util.ElasticSearchHandler;
import com.ultrapower.scheduler.util.TimeTool;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: xlt
 * @Description: 月调度必须在本月头两天内执行
 * @Date: Created in 11:09 2018/8/14
 */
public class convergeDayDataJob implements Job {

    private static Logger log = Logger.getLogger(convergeDayDataJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

//        当前时间
        Long currentTime = System.currentTimeMillis()-2*60*60*1000;
//        原始粒度的索引名
        String indexName = CoreConfig.RAW_TABLE_NAME;
        String indexRawName=CoreConfig.INDEX_RAW_DAY_NAME;

        List<JSONObject> jsonObjects = new ArrayList<>();

            Long startTime = TimeTool.getDayDayStart(currentTime);
            Long endTime = TimeTool.getDayDayEnd(currentTime);

            List<JSONObject> rawDataMethod = ElasticSearchHandler.getRawDataMethod(indexName,
                    "dcTime",
                    startTime,
                    endTime,
                    "busType.keyword",
                    "couserType.keyword",
                    "countNum",
                    "successNum",
                    "failNum",
                    "timeOutNum",
                    "avgTime");

            jsonObjects.addAll(rawDataMethod);

        ElasticSearchHandler.createIndexBulkResponse(indexRawName,"rawHour",jsonObjects);

    }


}
