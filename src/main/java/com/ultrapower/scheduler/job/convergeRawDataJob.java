package com.ultrapower.scheduler.job;

import com.ultrapower.scheduler.model.CoreConfig;
import com.ultrapower.scheduler.task.courseAndBusTask;
import com.ultrapower.scheduler.util.TimeTool;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.Executors.*;

/**
 * @Author: xlt
 * @Description:
 * @Date: Created in 17:50 2018/8/13
 */
public class convergeRawDataJob implements Job {

    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(convergeRawDataJob.class);
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        long currentTime = System.currentTimeMillis();
        Integer j =CoreConfig.JOB_CONVERGE_TIMES;
        String indexName=CoreConfig.BUSLOG_INDEXNAME+TimeTool.getTableName(CoreConfig.INDEX_INTERNAL);
        ExecutorService newCachedThreadPool = newCachedThreadPool();

        if (j >0 && j !=null){
            for (int i=0 ;i< j;i++){
                Long starttime=((currentTime - CoreConfig.BASELINE_TIME) / CoreConfig.BASELINE_TIME) * CoreConfig.BASELINE_TIME-CoreConfig.BASELINE_TIME*i;
                Long endtime=starttime+CoreConfig.BASELINE_TIME;
                newCachedThreadPool.execute(new courseAndBusTask(indexName,starttime,endtime));
            }
        }else {

            System.out.println("设置的执行次数不合理。为："+j);

        }

        newCachedThreadPool.shutdown();

    }

    public static void main(String[] args) {

        long currentTime = 1525240203711L;
        Integer j =CoreConfig.JOB_CONVERGE_TIMES;
//        String indexName=CoreConfig.BUSLOG_INDEXNAME+TimeTool.getTableName(CoreConfig.INDEX_INTERNAL);
        String indexName="buslog_20180430_20180506";
        ExecutorService newCachedThreadPool = newCachedThreadPool();
        if (j >0 && j !=null){
            System.out.println("j::::"+j);
            for (int i=0 ;i< j;i++){

                Long starttime=((currentTime - CoreConfig.BASELINE_TIME) / CoreConfig.BASELINE_TIME) * CoreConfig.BASELINE_TIME-CoreConfig.BASELINE_TIME*i;
                Long endtime=starttime+CoreConfig.BASELINE_TIME;

                System.out.println("startTime=="+starttime);
                newCachedThreadPool.execute(new courseAndBusTask(indexName,starttime,endtime));
//                new courseAndBusTask(indexName,starttime,endtime).run();
            }
        }else {
            System.out.println("设置的执行次数不合理。为："+j);
        }

        newCachedThreadPool.shutdown();
    }
}
