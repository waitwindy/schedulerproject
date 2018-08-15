package com.ultrapower.scheduler.model;

import java.io.Serializable;

/**
 *
 * @author wqf
 */
public  class UpLoadJob implements Serializable {

    public String name;
    public String desc;
    public String group;
    public String interval;
    public String jobClassName;
    public long lastScheduleTime;

    /** Creates a new instance of UpLoadJob */
    public UpLoadJob() {
    }

    public UpLoadJob(String name, String desc, String group, String interval, long lastScheduleTime) {
        this.name=name;
        this.desc=desc;
        this.group=group;
        this.interval=interval;
        this.lastScheduleTime=lastScheduleTime;
    }
    /**
     * 属性 desc 的获取方法。
     * @return 属性 desc 的值。
     */
    public String getDesc() {
        return desc;
    }

    /**
     * 属性 desc 的设置方法。
     * @param desc 属性 desc 的新值。
     */
    public void setDesc(String desc) {
        this.desc = desc;
    }

    /**
     * 属性 group 的获取方法。
     * @return 属性 group 的值。
     */
    public String getGroup() {
        return group;
    }

    /**
     * 属性 group 的设置方法。
     * @param group 属性 group 的新值。
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * 属性 interval 的获取方法。
     * @return 属性 interval 的值。
     */
    public String getInterval() {
        return interval;
    }

    /**
     * 属性 interval 的设置方法。
     * @param interval 属性 interval 的新值。
     */
    public void setInterval(String interval) {
        this.interval = interval;
    }

    /**
     * 属性 lastScheduleTime 的获取方法。
     * @return 属性 lastScheduleTime 的值。
     */
    public long getLastScheduleTime() {
        return lastScheduleTime;
    }

    /**
     * 属性 lastScheduleTime 的设置方法。
     * @param lastScheduleTime 属性 lastScheduleTime 的新值。
     */
    public void setLastScheduleTime(long lastScheduleTime) {
        this.lastScheduleTime = lastScheduleTime;
    }

    /**
     * 属性 name 的获取方法。
     * @return 属性 name 的值。
     */
    public String getName() {
        return name;
    }

    /**
     * 属性 name 的设置方法。
     * @param name 属性 name 的新值。
     */
    public void setName(String name) {
        this.name = name;
    }
    public String toString(){
        return    name+"(interval="+interval+",lastScheduleTime="+lastScheduleTime+",Class="+jobClassName+")";
    }

    /**
     * 属性 jobClassName 的获取方法。
     * @return 属性 jobClassName 的值。
     */
    public String getJobClassName() {
        return jobClassName;
    }

    /**
     * 属性 jobClassName 的设置方法。
     * @param jobClassName 属性 jobClassName 的新值。
     */
    public void setJobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
    }

}