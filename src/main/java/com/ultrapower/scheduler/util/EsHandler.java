package com.ultrapower.scheduler.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONPOJOBuilder;
import com.ultrapower.scheduler.model.CoreConfig;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.security.KeyStore;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @Author: xlt
 * @Description:
 * @Date: Created in 16:11 2018/8/10
 */
public class EsHandler {

    static Map<String, String> m = new HashMap<String, String>();
    // 设置client.transport.sniff为true来使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中�?
    static Settings settings = Settings.builder()
            .put("path.home", ".")
            .put("cluster.name", CoreConfig.ES_CLUSTERNAME)
            .put("client.transport.sniff", true)
//            .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE, true)
//            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE, true)
//            .put("searchguard.ssl.transport.enabled", true)
//            //ultrabdms1-keystore.jks文件需要从ES节点的elasticsearch-2.3.2/plugins/search-guard-2/sgconfig目录复制到本地
//            .put("searchguard.ssl.transport.keystore_filepath", CoreConfig.ES_KEYSTORE_FILENAME)
//            //truststore.jks文件需要从ES节点的elasticsearch-2.3.2/plugins/search-guard-2/sgconfig目录复制到本地
//            .put("searchguard.ssl.transport.truststore_filepath", CoreConfig.ES_TRUSTSTORE_FILENAME)
//            //生成证书时对应ultrabdms1-keystore.jks的密码
//            .put("searchguard.ssl.transport.keystore_password", CoreConfig.ES_KEYSTORE_PASSWORD)
//            //生成证书时对应truststore.jks的密码
//            .put("searchguard.ssl.transport.truststore_password", CoreConfig.ES_TRUSTSTORE_PASSWORD)
//            .put("searchguard.ssl.transport.enforce_hostname_verification", false)
            .build();

    // 创建私有对象
    private static TransportClient client;

    static {
        try {

            System.out.println(settings.getAsMap());
            client = new PreBuiltTransportClient(settings);
            String ip[] = CoreConfig.ES_IPS.split(Pattern.quote(","));
            String port[] = CoreConfig.ES_PORTS.split(Pattern.quote(","));
            for (int i = 0; i < ip.length; i++) {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip[i]), Integer.parseInt(port[i])));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    取得实例
    public static synchronized TransportClient getTransportClient() {
        return client;
    }

    //    获取环节的数据的方法。
    public static JSONObject getCourseNum(String indexName, String busTypeColum, String busType, String courseType, String startTimeColum, String costTimeColum, String timeOutColum, String stautusColum, Long startTime, Long endTime) {

        TransportClient client = ElasticSearchHandler.getTransportClient();

        BoolQueryBuilder must = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(busTypeColum, busType))
                .must(QueryBuilders.rangeQuery(startTimeColum).gt(startTime).lt(endTime));
        SearchRequestBuilder builder = client.prepareSearch(indexName).setQuery(must);

        long value;
        long success;
        long fail;
        double avg1;
        long timeOut;

        JSONObject object = new JSONObject();

        AggregationBuilder count = AggregationBuilders.count("countbuslog").field(costTimeColum);
        AggregationBuilder successfilter = AggregationBuilders.filter("success", QueryBuilders.termsQuery(stautusColum, "1"));
        AggregationBuilder failfilter = AggregationBuilders.filter("fail", QueryBuilders.termsQuery(stautusColum, "0"));
        AggregationBuilder timeOutAgg = AggregationBuilders.filter("timeOut", QueryBuilders.termsQuery(timeOutColum, "1"));
        AggregationBuilder avg = AggregationBuilders.avg("avg").field(costTimeColum);

        builder.addAggregation(count).addAggregation(successfilter).addAggregation(failfilter).addAggregation(avg).addAggregation(timeOutAgg);
        SearchResponse response = builder.execute().actionGet();

        value = ((InternalValueCount) response.getAggregations().get("countbuslog")).getValue();
        success = ((InternalFilter) response.getAggregations().get("success")).getDocCount();
        fail = ((InternalFilter) response.getAggregations().get("fail")).getDocCount();
        avg1 = ((InternalAvg) response.getAggregations().get("avg")).getValue();
        timeOut = ((InternalFilter) response.getAggregations().get("timeOut")).getDocCount();

        object.put("allCountNum", value);
        object.put("busType", busType);
        object.put("courseType", courseType);
        object.put("successNum", success);
        object.put("failNum", fail);
        object.put("avgCostTime", avg1);
        object.put("timeOutNum", timeOut);

        return object;
    }

    //      按照业务进行分组。然后按照分组，按照环节限定时间进行查询。
    public static List<JSONObject> getCourseNum2(String indexName, String courseType, String busTypeColume, String courseTimeColume, Long startTime, Long endTime, String courseStatusColume, String courseIsOutTimeColume,String courseCostTimeColume) {
        List<JSONObject> jsonObjects = new ArrayList<JSONObject>();
        TransportClient client = ElasticSearchHandler.getTransportClient();
        TermsAggregationBuilder field = AggregationBuilders.terms("busterms").field(busTypeColume);
        FiltersAggregationBuilder countAgg = AggregationBuilders.filters("timefilter1", QueryBuilders.rangeQuery(courseTimeColume).gt(startTime).lt(endTime)).subAggregation(AggregationBuilders.count("count").field(courseTimeColume));
        FiltersAggregationBuilder statusAgg = AggregationBuilders.filters("timefilter2", QueryBuilders.rangeQuery(courseTimeColume).gt(startTime).lt(endTime)).subAggregation(AggregationBuilders.terms("status").field(courseStatusColume));
        FiltersAggregationBuilder timeIsOutAgg = AggregationBuilders.filters("timefilter3", QueryBuilders.rangeQuery(courseTimeColume).gt(startTime).lt(endTime)).subAggregation(AggregationBuilders.terms("status").field(courseIsOutTimeColume));
        FiltersAggregationBuilder avgTimeAgg = AggregationBuilders.filters("timefilter4", QueryBuilders.rangeQuery(courseTimeColume).gt(startTime).lt(endTime)).subAggregation(AggregationBuilders.avg("avgTime").field(courseCostTimeColume));

        SearchResponse allAgg = client.prepareSearch(indexName).addAggregation(field.subAggregation(countAgg).subAggregation(statusAgg).subAggregation(timeIsOutAgg).subAggregation(avgTimeAgg)).execute().actionGet();
        Terms terms = (Terms) allAgg.getAggregations().get("busterms");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            JSONObject object = new JSONObject();
            String busTypeStr = bucket.getKeyAsString();
            object.put("busType", busTypeStr);
            object.put("couserType", courseType);
            InternalFilters countfilter = (InternalFilters) bucket.getAggregations().get("timefilter1");
            List<InternalFilters.InternalBucket> countBuckets = countfilter.getBuckets();
            for (InternalFilters.InternalBucket bu : countBuckets) {
                String keyAsString = bu.getKeyAsString();
                long count = ((InternalValueCount) bu.getAggregations().get("count")).getValue();
                object.put("countNum", count);
            }
            InternalFilters statusfilter = (InternalFilters) bucket.getAggregations().get("timefilter2");
            List<InternalFilters.InternalBucket> statusBuckets = statusfilter.getBuckets();
            for (InternalFilters.InternalBucket bu : statusBuckets) {
                Terms status = (Terms) bu.getAggregations().get("status");
                for (Terms.Bucket statusBucket : status.getBuckets()) {
                    long docCount = statusBucket.getDocCount();
                    if (statusBucket.getKeyAsString().equals("0")) {
                        object.put("successNum", docCount);
                    } else {
                        object.put("failNum", docCount);
                    }

                }
            }

            InternalFilters timeIsOutfilter = (InternalFilters) bucket.getAggregations().get("timefilter3");
            for (InternalFilters.InternalBucket bu : timeIsOutfilter.getBuckets()) {
                Terms status = (Terms) bu.getAggregations().get("status");
                for (Terms.Bucket statusBucket : status.getBuckets()) {
                    long docCount = statusBucket.getDocCount();

                    if ("0".equals(statusBucket.getKeyAsString())) {
                        object.put("timeOutNum", docCount);
                    } else {
                        object.put("notTimeOutNum", docCount);
                    }

                }
            }

            InternalFilters avgTimefilter = (InternalFilters) bucket.getAggregations().get("timefilter4");
//            System.out.println("avgTimefilter size====" + avgTimefilter.getBuckets().size());
            for (InternalFilters.InternalBucket bu : avgTimefilter.getBuckets()) {
                double avgTime = ((InternalAvg) bu.getAggregations().get("avgTime")).getValue();
                object.put("avgTime", avgTime);
            }

            jsonObjects.add(object);
        }

        return jsonObjects;
    }



    public static List<String>  getAllCouse(String indexName){

        TransportClient client = ElasticSearchHandler.getTransportClient();
        ImmutableOpenMap<String, MappingMetaData> mappings = client.admin().cluster().prepareState().execute()
                .actionGet().getState().getMetaData().getIndices().get("buslog_20180430_20180506").getMappings();
        String s = mappings.get("all").get().source().toString();
        JSONObject jsonObject = JSON.parseObject(s);
        JSONObject jsonObject1 = JSON.parseObject(jsonObject.get("all").toString());
        JSONObject keys = JSON.parseObject(jsonObject1.get("properties").toString());
//   获取到所有字段。
        Set<String> strings = keys.keySet();
        List<String> allCourse = strings.stream().filter(line -> line.contains("_") && line.contains("success")).map(s2 -> s2.split("_")[0]).collect(Collectors.toList());

        return  allCourse;
    }

    public static void putDataToEs(List<JSONObject> dataList){

    }


    public static void main(String[] args) throws UnsupportedEncodingException {

//    getAllCouse("buslog_20180430_20180506");

//        List<JSONObject> objects = getCourseNum2("buslog_20180430_20180506", "web", "web_menuname.keyword", "web_starttime", 1525240203711L, 1525270203711L, "success", "web_isoutTime", "web_costtime");
//        System.out.println(objects);

    createIndexResponse("test4","test03","{\"couserType\":\"web\",\"notTimeOutNum\":156,\"failNum\":427,\"countNum\":57654,\"avgTime\":153.58566621570057,\"successNum\":57227,\"timeOutNum\":57498,\"busType\":\"实时话费查询\"}");

    }


    public static IndexResponse createIndexResponse(String indexname, String type, String jsondata) throws UnsupportedEncodingException {
        IndexResponse response = getTransportClient().prepareIndex(indexname, type)
                .setSource(jsondata.getBytes("gbk"))
                .execute()
                .actionGet();
        return response;
    }

}
