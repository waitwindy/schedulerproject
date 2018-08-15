package com.ultrapower.scheduler.model;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2015/10/27 0027.
 */
public class CollectConf {

    public static List<UpLoadJob> jobs;

    public static List<UpLoadJob> internalJob;

    public static List<String> kbpClassList;

    private static final String PROPERTIES = "bigdata.xml";

    private static final Logger log = LoggerFactory.getLogger(CollectConf.class);

    static {
        InputStream in = CollectConf.class.getClassLoader().getResourceAsStream(PROPERTIES);
        SAXReader saxReader = new SAXReader();
        try {
            Document doc = saxReader.read(in);

            List jobNodes = doc.selectNodes("/root/jobs/Schedule");
            if (jobNodes != null) {
                jobs = new ArrayList<UpLoadJob>();
                for (int i = 0, size = jobNodes.size(); i < size; i++) {
                    Element element = (Element) jobNodes.get(i);
                    String name = element.attribute("name").getText();
                    String interval = element.attribute("interval").getText();
                    String classname = element.attribute("class").getText();
                    String group = element.attribute("group").getText();
                    long lastScheduleTime = 0;
                    UpLoadJob ujob = new UpLoadJob(name, name, group, interval, lastScheduleTime);
                    ujob.setJobClassName(classname);
                    jobs.add(ujob);
                }
            }

            List internaNodes = doc.selectNodes("root/internals/Schedule");

            if (internaNodes != null) {

                internalJob = new ArrayList<UpLoadJob>();

                for (int i = 0, size = internaNodes.size(); i < size; i++) {
                    Element element = (Element) internaNodes.get(i);
                    String name = element.attribute("name").getText();
                    String interval = element.attribute("interval").getText();
                    String classname = element.attribute("class").getText();
                    String group = element.attribute("group").getText();
                    long lastScheduleTime = 0;
                    UpLoadJob ujob = new UpLoadJob(name, name, group, interval, lastScheduleTime);
                    ujob.setJobClassName(classname);
                    internalJob.add(ujob);
                }
            }
        } catch (Exception e) {
            log.error(PROPERTIES + "文件解析失败!", e);
            e.printStackTrace();
        }
    }

}
