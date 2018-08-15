package com.ultrapower.scheduler.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ultrapower.scheduler.model.CoreConfig;
import com.ultrapower.scheduler.util.ElasticSearchHandler;
import com.ultrapower.scheduler.util.TimeTool;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.junit.Test;

import java.util.*;

/**
 * @Author: xlt
 * @Description:
 * @Date: Created in 13:45 2018/8/10
 */
public class javaTest {

    /**
     * 测试调用查询方法，汇聚查询的工作
     */

    @Test
    public void test01() {

//        TransportClient client = ElasticSearchHandler.getTransportClient();
//        String indexname = CoreConfig.BUSLOG_INDEXNAME + TimeTool.getTableName(CoreConfig.INDEX_INTERNAL);
        List<JSONObject> courseNum = getCourseNum("buslog_20180430_20180506", "web_menuid.keyword", "web", "starttime", "brower_costtime", "isoutTime", "service_success", 1525240203711L, 1525270203711L);
        System.out.println(courseNum);

    }

    //   分业务进行统计 按照业务进行分类。
    public static List<JSONObject> getCourseNum(String indexName, String busTypeColum, String courseType, String startTimeColum, String costTimeColum, String timeOutColum, String stautusColum, Long startTime, Long endTime) {

        List<JSONObject> list = new ArrayList<JSONObject>();
        TransportClient client = ElasticSearchHandler.getTransportClient();
        BoolQueryBuilder must = QueryBuilders.boolQuery()
//                .must(QueryBuilders.termQuery(busTypeColum, busType))
                .must(QueryBuilders.rangeQuery(startTimeColum).gt(startTime).lt(endTime));
        SearchRequestBuilder builder = client.prepareSearch(indexName).setQuery(must);
        TermsAggregationBuilder bustype = AggregationBuilders.terms("bustype").field(busTypeColum);
        AggregationBuilder count = AggregationBuilders.count("countbuslog").field(costTimeColum);
        AggregationBuilder successfilter = AggregationBuilders.filter("success", QueryBuilders.termsQuery(stautusColum, "1"));
        AggregationBuilder failfilter = AggregationBuilders.filter("fail", QueryBuilders.termsQuery(stautusColum, "0"));
        AggregationBuilder timeOutAgg = AggregationBuilders.filter("timeOut", QueryBuilders.termsQuery(timeOutColum, "1"));
        AggregationBuilder avg = AggregationBuilders.avg("avg").field(costTimeColum);
        builder.addAggregation(bustype.subAggregation(count)
                .subAggregation(successfilter)
                .subAggregation(failfilter)
                .subAggregation(avg)
                .subAggregation(timeOutAgg));
        SearchResponse response = builder.execute().actionGet();
        Terms terms = (Terms) response.getAggregations().get("bustype");

        for (Terms.Bucket term : terms.getBuckets()) {

            String busType = term.getKeyAsString();
            JSONObject object = new JSONObject();
            long value = ((InternalValueCount) (term.getAggregations().get("countbuslog"))).getValue();
            long success = ((InternalFilter) term.getAggregations().get("success")).getDocCount();
            long fail = ((InternalFilter) term.getAggregations().get("fail")).getDocCount();
            double avg1 = ((InternalAvg) term.getAggregations().get("avg")).getValue();
            long timeOut = ((InternalFilter) term.getAggregations().get("timeOut")).getDocCount();

            object.put("busType", busType);
            object.put("courseType", courseType);
            object.put("allCountNum", value);
            object.put("successNum", success);
            object.put("failNum", fail);
            object.put("avgCostTime", avg1);
            object.put("timeOutNum", timeOut);
            list.add(object);

        }

        return list;
    }

    //  查询所有的业务类型
    public static List<String> getAllBus(String indexName, String busNamColum) {
        List<String> busNames = new ArrayList<String>();
        SearchResponse response = ElasticSearchHandler.getTransportClient().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("busName").field(busNamColum)).execute().actionGet();
        Terms terms = (Terms) response.getAggregations().get("busName");

        for (Terms.Bucket term : terms.getBuckets()) {
            String busName = term.getKeyAsString();
            busNames.add(busName);
        }
        return busNames;
    }

    //    获取环节的数据。
    public static List<JSONObject> getCouserMsg(String indexName, String busTypeColum, List<String> busNames, List<String> courseNames, Long startTime, Long endTime) {

        List<JSONObject> list = new ArrayList<JSONObject>();

        for (String busName : busNames) {
            for (String courseName : courseNames) {
                String timeColume = courseName + "_startTime";
                String costColume = courseName + "_costTime";
                String statusColume = courseName + "_status";
                String isTimeOutColume = courseName + "_isTimeOut";

                SearchResponse response = ElasticSearchHandler.getTransportClient()
                        .prepareSearch(indexName)
                        .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(busTypeColum, busName))
                                .must(QueryBuilders.rangeQuery(timeColume).gt(startTime).lt(endTime)))
                        .addAggregation(AggregationBuilders.count("countbuslog").field(costColume))
                        .addAggregation(AggregationBuilders.filter("success", QueryBuilders.termsQuery(statusColume, "1")))
                        .addAggregation(AggregationBuilders.filter("fail", QueryBuilders.termsQuery(statusColume, "0")))
                        .addAggregation(AggregationBuilders.filter("timeOut", QueryBuilders.termsQuery(isTimeOutColume, "1")))
                        .addAggregation(AggregationBuilders.avg("avg").field(costColume)).execute().actionGet();

                long value = ((InternalValueCount) (response.getAggregations().get("countbuslog"))).getValue();
                long success = ((InternalFilter) response.getAggregations().get("success")).getDocCount();
                long fail = ((InternalFilter) response.getAggregations().get("fail")).getDocCount();
                double avg1 = ((InternalAvg) response.getAggregations().get("avg")).getValue();
                long timeOut = ((InternalFilter) response.getAggregations().get("timeOut")).getDocCount();

                JSONObject object = new JSONObject();
                object.put("busType", busName);
                object.put("courseType", courseName);
                object.put("allCountNum", value);
                object.put("successNum", success);
                object.put("failNum", fail);
                object.put("avgCostTime", avg1);
                object.put("timeOutNum", timeOut);

                list.add(object);

            }

        }

        return list;

    }

    //    按照业务进行分类统计。
    public static List<JSONObject> getBusLogmsg(String indexName, String busTypeColum, String costTimeColum, String stautusColum, String isTimeOutColume, String startTimeColum, Long startTime, Long endTime) {

        List<JSONObject> list = new ArrayList<JSONObject>();
        SearchResponse response = ElasticSearchHandler.getTransportClient().prepareSearch(indexName)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.rangeQuery(startTimeColum).gt(startTime).lt(endTime)))
                .addAggregation(AggregationBuilders.terms("busNames").field(busTypeColum)
                        .subAggregation(AggregationBuilders.count("countbuslog").field(costTimeColum))
                        .subAggregation(AggregationBuilders.filter("success", QueryBuilders.termsQuery(stautusColum, "1")))
                        .subAggregation(AggregationBuilders.filter("fail", QueryBuilders.termsQuery(stautusColum, "0")))
                        .subAggregation(AggregationBuilders.filter("timeOut", QueryBuilders.termsQuery(isTimeOutColume, "1")))
                        .subAggregation(AggregationBuilders.avg("avg").field(costTimeColum))).execute().actionGet();

        Terms terms = (Terms) response.getAggregations().get("busNames");

        for (Terms.Bucket term : terms.getBuckets()) {
            String busType = term.getKeyAsString();
            JSONObject object = new JSONObject();
            long value = ((InternalValueCount) (term.getAggregations().get("countbuslog"))).getValue();
            long success = ((InternalFilter) term.getAggregations().get("success")).getDocCount();
            long fail = ((InternalFilter) term.getAggregations().get("fail")).getDocCount();
            double avg1 = ((InternalAvg) term.getAggregations().get("avg")).getValue();
            long timeOut = ((InternalFilter) term.getAggregations().get("timeOut")).getDocCount();

            object.put("busType", busType);
//                object.put("courseType", "null");
            object.put("allCountNum", value);
            object.put("successNum", success);
            object.put("failNum", fail);
            object.put("avgCostTime", avg1);
            object.put("timeOutNum", timeOut);

            list.add(object);
        }

        return list;
    }

    public static List<JSONObject> getRawDataMethod(String indexName,String timeColume,Long startTime,Long endTime,String busTypeColume,String courseTypeColume,String countNumColume,String successColume,String failNumColume,String timeOutNumColume,String avgTimeColume) {

        TransportClient client = ElasticSearchHandler.getTransportClient();
        BoolQueryBuilder time = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(timeColume).gt(startTime).lt(endTime));

        List<JSONObject> jsonObjects = new ArrayList<>();
        SearchResponse response = client.prepareSearch(indexName)
                .setQuery(time)
                .addAggregation(
                        AggregationBuilders.terms("bustype").field(busTypeColume)
                                .subAggregation(AggregationBuilders.terms("courseType").field(courseTypeColume)
                                        .subAggregation(AggregationBuilders.sum("allNum").field(countNumColume))
                                        .subAggregation(AggregationBuilders.sum("success").field(successColume))
                                        .subAggregation(AggregationBuilders.sum("failNum").field(failNumColume))
                                        .subAggregation(AggregationBuilders.sum("isTimeOut").field(timeOutNumColume))
                                        .subAggregation(AggregationBuilders.avg("avgTime").field(avgTimeColume)))).execute().actionGet();
        Terms terms = (Terms) response.getAggregations().get("bustype");

        for (Terms.Bucket bustype:terms.getBuckets()) {
            String busKey = bustype.getKeyAsString();
            Terms courseTypes = (Terms) bustype.getAggregations().get("courseType");
            for (Terms.Bucket courseType:courseTypes.getBuckets()) {
                String courseKey = courseType.getKeyAsString();

                JSONObject object = new JSONObject();
                object.put("busType",busKey);
                object.put("courseType",courseKey);

                Double allNum = ((InternalSum) courseType.getAggregations().get("allNum")).getValue();
                Double success = ((InternalSum) courseType.getAggregations().get("success")).getValue();
                Double failNum = ((InternalSum) courseType.getAggregations().get("failNum")).getValue();
                Double isTimeOut =((InternalSum) courseType.getAggregations().get("isTimeOut")).getValue();
                Double avgTime = ((InternalAvg) courseType.getAggregations().get("avgTime")).getValue();

                if (!allNum.isNaN()){
                    object.put("countNum",allNum);
                }

                if (!success.isNaN()){
                    object.put("successNum",success);
                }

                if (!failNum.isNaN()){
                    object.put("failNum",failNum);
                }

                if (!isTimeOut.isNaN()){
                    object.put("timeOutNum",isTimeOut);
                }

                if (!avgTime.isNaN()){
                    object.put("avgTime",avgTime);
                }

                jsonObjects.add(object);
            }

        }

        return jsonObjects;

    }

    @Test
    public void testHourMethod(){
        List<JSONObject> rawHourMethod = getRawDataMethod("raw_endtoend","dcTime",1525239500000L,1525239910000L,"busType.keyword","couserType.keyword","countNum","successNum","failNum","timeOutNum","avgTime");
        System.out.println(rawHourMethod);
    }

    @Test
    public void testInsert(){
        Set<String> strings = new HashSet<>();
        List<JSONObject> jsonObjects = new ArrayList<>();

        for (int i = 0; i < 100; i++) {

            JSONObject object = new JSONObject();
            object.put("id",i);
            object.put("name",UUID.randomUUID());
            jsonObjects.add(object);
        }
        ElasticSearchHandler.createIndexBulkResponse("test05","all",jsonObjects);
    }

    @Test
    public void testIndxExit(){
        List<String> allCourse = ElasticSearchHandler.getAllCourse("buslog_20180326_20180421");
        System.out.println(allCourse);
    }
}
