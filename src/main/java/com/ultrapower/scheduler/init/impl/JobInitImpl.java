package com.ultrapower.scheduler.init.impl;

import com.ultrapower.scheduler.model.CollectConf;
import com.ultrapower.scheduler.model.CoreConfig;
import com.ultrapower.scheduler.model.UpLoadJob;

import static org.quartz.CronScheduleBuilder.cronSchedule;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.apache.log4j.Logger;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.List;

/**
 * @author wqf
 * @version :4.0
 * @desc :调度初始化
 * @date:2015-9-2 下午3:49:07
 */
public class JobInitImpl implements InitInterface {

    private static Logger log = Logger.getLogger(JobInitImpl.class);

    public boolean checkInit() {
        return true;
    }

    public boolean close() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public boolean init() {
        try {
            SchedulerFactory sf = new org.quartz.impl.StdSchedulerFactory();
            Scheduler scheduler = sf.getScheduler();
            scheduler.start();

            List<UpLoadJob> jobList = CollectConf.jobs;
            //往调度里增加任务
            for (UpLoadJob ujob : jobList) {

                String jobname = ujob.getName();
                String interval = ujob.getInterval();
                String jobgroup = ujob.getGroup();
                String desc = ujob.getDesc();
                String jobClass = ujob.getJobClassName();
                long lastScheduleTime = ujob.getLastScheduleTime();
                Class cls = Class.forName(jobClass);

                JobDetail job = newJob(cls).withIdentity(jobname, jobgroup).build();
                CronTrigger cronTrigger = newTrigger().withIdentity(jobname + "CronTrigger", jobgroup).withSchedule(cronSchedule(interval)).build();
                job.getJobDataMap().put("lastScheduleTime", lastScheduleTime);
                scheduler.scheduleJob(job, cronTrigger);
                System.out.println("启动调度" + jobname + "(" + desc + "|" + interval + "|" + jobClass + "|" + lastScheduleTime + ")");
            }

//            是否启动小时天月的调度程序
            if (CoreConfig.DO_INTERVAL) {

                List<UpLoadJob> internalJob = CollectConf.internalJob;
                for (UpLoadJob ujob : internalJob) {

                    String jobname = ujob.getName();
                    String interval = ujob.getInterval();
                    String jobgroup = ujob.getGroup();
                    String desc = ujob.getDesc();
                    String jobClass = ujob.getJobClassName();
                    long lastScheduleTime = ujob.getLastScheduleTime();
                    Class cls = Class.forName(jobClass);

                    JobDetail job = newJob(cls).withIdentity(jobname, jobgroup).build();
                    CronTrigger cronTrigger = newTrigger().withIdentity(jobname + "CronTrigger", jobgroup).withSchedule(cronSchedule(interval)).build();
                    job.getJobDataMap().put("lastScheduleTime", lastScheduleTime);
                    scheduler.scheduleJob(job, cronTrigger);
                    System.out.println("启动调度" + jobname + "(" + desc + "|" + interval + "|" + jobClass + "|" + lastScheduleTime + ")");
                }
            }

        } catch (Exception e) {
            System.err.println("调度异常");
            e.printStackTrace();
            return false;
        }
        System.out.println("调度任务启动完毕！");
        return true;
    }

    public void start() {

    }

}
