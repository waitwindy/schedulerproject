package com.ultrapower.scheduler.task;

import com.alibaba.fastjson.JSONObject;
import com.ultrapower.scheduler.util.ElasticSearchHandler;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * @Author: xlt
 * @Description:
 * @Date: Created in 16:01 2018/8/13
 */
public class excuteCourseTask implements Callable<List<JSONObject>> {

    private String indexName;
//   环节名称
    private String couserName;
//    业务类型列名
    private String busTypeColume;
//   开始时间
    private Long startTime;
//    结束时间
    private Long endTime;

    public excuteCourseTask(String indexName,String couserName,String busTypeColume,Long startTime,Long endTime){
        this.indexName=indexName;
        this.couserName=couserName;
        this.busTypeColume=busTypeColume;
        this.startTime=startTime;
        this.endTime=endTime;
    }
    @Override
    public List<JSONObject> call() throws Exception {

            String startTimeColum=couserName+"_starttime";
            String statusColume=couserName+"_success";
            String isOutTimeColume=couserName+"_isoutTime";
            String costTimeColume=couserName+"_costtime";

            List<JSONObject> courseValue = ElasticSearchHandler.getCourseNum2(indexName, couserName, busTypeColume, startTimeColum, startTime, endTime, statusColume, isOutTimeColume, costTimeColume);

        return courseValue;
    }
}
