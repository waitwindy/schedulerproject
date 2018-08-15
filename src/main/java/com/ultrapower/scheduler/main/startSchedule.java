package com.ultrapower.scheduler.main;

import com.ultrapower.scheduler.init.impl.JobInitImpl;
import com.ultrapower.scheduler.model.CoreConfig;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileLock;

/**
 * @Author: xlt
 * @Description: 启动调度任务的程序入口。
 * @Date: Created in 15:34 2018/8/14
 */
public class startSchedule {

    private static FileLock lock = null;

    public static void main(String args[]){

        startSchedule nms = new startSchedule();

        if (!nms.isRunning()) {
            System.err.println("Alert is already running!");
            return;
        }
        PropertyConfigurator.configure("D:\\schedulerproject\\src\\main\\resources\\log4j.properties");

        //启动调度
        new JobInitImpl().init();

    }

    // 判断该应用是否已启动
    protected boolean isRunning() {
        try {
            // 获得实例标志文件
            File flagFile = new File("UpLoadUtil.instance");
            // 如果不存在就新建一个
            if (!flagFile.exists()) {
                flagFile.createNewFile();
            }
            // 获得文件锁
            lock = new FileOutputStream("UpLoadUtil.instance").getChannel().tryLock();
            // 返回空表示文件已被运行的实例锁定
            if (lock == null) {
                return false;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return true;
    }
}
