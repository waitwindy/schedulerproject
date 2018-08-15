package com.ultrapower.scheduler.job;

import com.alibaba.fastjson.JSONObject;
import com.ultrapower.scheduler.model.CoreConfig;
import com.ultrapower.scheduler.util.ElasticSearchHandler;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: xlt
 * @Description: 汇聚小时粒度的数据
 * @Date: Created in 14:20 2018/8/14
 */
public class convergeHourDataJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

//        当前时间
        Long currentTime = System.currentTimeMillis();
//        原始粒度的索引名
        String indexName = CoreConfig.RAW_TABLE_NAME;
        String indexRawName=CoreConfig.INDEX_RAW_HOUR_NAME;
        int dotimes = CoreConfig.R_HOUR_CONVERGE_TIMES;

        List<JSONObject> jsonObjects = new ArrayList<>();

        for (int i = 0; i <= dotimes; i++) {
            Long startTime = (currentTime - 60 * 60 * 1000) / 60 * 60 * 1000 - 60 * 60 * 1000 * i;
            Long endTime = startTime + 60 * 60 * 1000;

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
        }

        ElasticSearchHandler.createIndexBulkResponse(indexRawName,"rawHour",jsonObjects);

    }
}
