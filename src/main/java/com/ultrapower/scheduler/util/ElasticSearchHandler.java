package com.ultrapower.scheduler.util;

//import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
//import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
//import com.utrapower.hnbigdata.dbutils.CommonBean;
//import com.utrapower.hnbigdata.dbutils.LinkDataBase;
//import com.utrapower.hnbigdata.model.CoreConfig;
//import com.utrapower.hnbigdata.model.EventInfo;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.oracle.jrockit.jfr.EventInfo;
import com.ultrapower.scheduler.model.CoreConfig;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
//import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
//import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
//import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearchHandler {

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
//            client= new PreBuiltTransportClient(settings,SearchGuardSSLPlugin.class);
            client= new PreBuiltTransportClient(settings);
            String ip[] = CoreConfig.ES_IPS.split(Pattern.quote(","));
            String port[] = CoreConfig.ES_PORTS.split(Pattern.quote(","));
            for(int i=0;i<ip.length;i++){
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip[i]), Integer.parseInt(port[i])));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 取得实例
    public static synchronized TransportClient getTransportClient() {
        return client;
    }

public static void createUpsertIndexBulk(String indexname, String type, Map<String,String> mapData){
        
        List<UpdateRequest> requests = new ArrayList<UpdateRequest>();
        for(Map.Entry<String,String> entry : mapData.entrySet()){
        IndexRequest indexRequest = new IndexRequest(indexname, type, entry.getKey()).source(entry.getValue());
        UpdateRequest updateRequest = new UpdateRequest(indexname, type, entry.getKey())
           .doc(entry.getValue())
           .upsert(indexRequest); 
        requests.add(updateRequest);
        }
       
           BulkRequestBuilder bulkRequest = getTransportClient().prepareBulk();
           
           for (UpdateRequest request : requests) {  
               bulkRequest.add(request);  
           }  
         
           BulkResponse bulkResponse = bulkRequest.execute().actionGet();  
           if (bulkResponse.hasFailures()) {  
               System.err.println("批量创建索引错误！"); 
           }
            
       }

    
    /**
     * 批量插入文档
     * @param indexname
     * @param type
     * @param setData
     */
    public static void createIndexBulkResponse(String indexname, String type, Set<String> setData){
        //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
        //IndexRequestBuilder requestBuilder = client.prepareIndex(indexname, type).setRefresh(true);
    	List<IndexRequest> requests = new ArrayList<IndexRequest>();
        for(String value : setData){
            IndexRequest request = getTransportClient()
                    .prepareIndex(indexname, type).setSource(value)
                    .request();  
      
            requests.add(request);
            
        } 
        BulkRequestBuilder bulkRequest = getTransportClient().prepareBulk();
        
        for (IndexRequest request : requests) {  
            bulkRequest.add(request);  
        }  
      
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();  
        if (bulkResponse.hasFailures()) {  
            System.err.println("批量创建索引错误！"); 
        }
         
    }

    public static void createIndexBulkResponse(String indexname, String type, List<JSONObject> setData) {

        //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
        //IndexRequestBuilder requestBuilder = client.prepareIndex(indexname, type).setRefresh(true);
        List<IndexRequest> requests = new ArrayList<IndexRequest>();
        for(JSONObject value : setData){

            String id =value.getString("dcTime")+value.getString("busType")+value.get("couserType");

            IndexRequest request = null;
            request = getTransportClient()
                    .prepareIndex(indexname, type,id).setSource(value)
                    .request();

            requests.add(request);

        }
        BulkRequestBuilder bulkRequest = getTransportClient().prepareBulk();

        for (IndexRequest request : requests) {
            bulkRequest.add(request);
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();

        if (bulkResponse.hasFailures()) {
            String s = bulkResponse.toString();
            System.err.println(s+" create index error");
        }

    }


    /**
     * 批量插入文档
     * @param indexname
     * @param type
     * @param mapData
     */
    public static void createIndexBulkResponse(String indexname, String type, HashMap<String,String> mapData){
        //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
        //IndexRequestBuilder requestBuilder = client.prepareIndex(indexname, type).setRefresh(true);
        List<IndexRequest> requests = new ArrayList<IndexRequest>();
        for(Map.Entry<String,String> entry : mapData.entrySet()){
            IndexRequest request = getTransportClient()
                    .prepareIndex(indexname, type,entry.getKey()).setSource(entry.getValue())
                    .request();

            requests.add(request);

        }
        BulkRequestBuilder bulkRequest = getTransportClient().prepareBulk();

        for (IndexRequest request : requests) {
            bulkRequest.add(request);
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            System.err.println("批量创建索引错误！");
            System.out.println(bulkResponse.buildFailureMessage());
        }
    }
    
    /**
     * 单条插入文档
     * @param
     * @param jsondata
     * @return
     */
    public static  IndexResponse createIndexResponse(String indexname, String type,String jsondata){
        IndexResponse response = getTransportClient().prepareIndex(indexname, type)
                .setSource(jsondata)
            .execute()
            .actionGet();
        return response;
    }


    /**
     * 单条插入文档
     * @param
     * @param jsondata
     * @return
     */
    public static  IndexResponse createIndexResponse(String indexname, String type,String jsondata,String id){
        IndexResponse response = getTransportClient().prepareIndex(indexname, type,id)
                .setSource(jsondata)
                .execute()
                .actionGet();
        return response;
    }

    
    /**
     * 执行搜索
     * @param queryBuilder
     * @param indexname
     * @param type
     * @return
     */
    public static List<String>  searcher(QueryBuilder queryBuilder, String indexname, String type){
        List<String> list = new ArrayList<String>();

        //查询那个索引的那个类型
        SearchResponse searchResponse = getTransportClient().prepareSearch(indexname).setTypes(type)
        .setQuery(queryBuilder)
        .setSize(Integer.MAX_VALUE)
        .execute()
        .actionGet();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for(int i=0;i<searchHists.length;i++){
            list.add(searchHists[i].getSourceAsString());
        }
        return list;
    }



    public static List<String>  searcherSetSize(QueryBuilder queryBuilder, String indexname, String type){
        List<String> list = new ArrayList<String>();

        //查询那个索引的那个类型
        SearchResponse searchResponse = getTransportClient().prepareSearch(indexname).setTypes(type)
                .setQuery(queryBuilder).setSize(Integer.MAX_VALUE)
                .execute()
                .actionGet();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for(int i=0;i<searchHists.length;i++){
            list.add(searchHists[i].getSourceAsString());
        }
        return list;
    }
    
    /**
     * 分组搜索
     * @param queryBuilder
     * @param indexname
     * @param type
     * @return
     */
    public static Terms  searcherAggregation(QueryBuilder queryBuilder, String indexname, String type){

        SearchRequestBuilder setSize = getTransportClient().prepareSearch(indexname).setTypes(type);
        SearchResponse actionGet = setSize.addAggregation(AggregationBuilders.terms( "web_menuname").field("web_busname.keyword")
        		.subAggregation(AggregationBuilders.avg("avgValue").field("web_costtime"))
				.subAggregation(AggregationBuilders.sum("sumValue").field("web_success.keyword"))
				.subAggregation(AggregationBuilders.sum("sumoutTimeValue").field("isoutTime.keyword"))
				).execute().actionGet();
        Terms terms = actionGet.getAggregations().get("web_menuname");

        return terms;
    }
    public static Terms searchAggregation2(QueryBuilder queryBuilder, String indexname, String type){

        SearchRequestBuilder builder = ElasticSearchHandler.getTransportClient().prepareSearch(indexname).setTypes("type");

//       按照web_busname进行分组
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("count").field("web_busname.keyword");
//      求web_costtime字段的平均值
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg("avgValue").field("web_costtime");
//      求success 字段的总值
        SumAggregationBuilder successAgg = AggregationBuilders.sum("sumValue").field("success");

        SearchRequestBuilder requestBuilder = builder.setQuery(queryBuilder).addAggregation(aggregationBuilder.subAggregation(avgAgg).subAggregation(successAgg));

        SearchResponse response = builder.execute().actionGet();
        Terms count = response.getAggregations().get("count");
        return count;
    }

public static List<String> scrollSearcher(QueryBuilder qb, String indexname, String type){
        
	    List<String> list = new ArrayList<String>();
	    SearchResponse scrollResp = client.prepareSearch(indexname).setTypes(type)
//		        .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
		        .addSort(SortBuilders.scoreSort())
		        .setScroll(new TimeValue(60000))
		        .setQuery(qb)
		        .setSize(10000).execute().actionGet();

		while (true) {

		    for (SearchHit hit : scrollResp.getHits().getHits()) {
		        //Handle the hit...
		    	list.add(hit.getSourceAsString());
		    }
		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
		    //Break condition: No hits are returned
		    if (scrollResp.getHits().getHits().length == 0) {
		        break;
		    }
		}
		 return list;
    }
    /**
     * 查询数量
     * @param queryBuilder
     * @param indexname
     * @param type
     * @return
     */
    public static Long  count(QueryBuilder queryBuilder, String indexname, String type){

        List<String> list = new ArrayList<String>();
        SearchResponse scrollResp = client.prepareSearch(indexname).setTypes(type)
//		        .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .addSort(SortBuilders.scoreSort())
                .setScroll(new TimeValue(60000))
                .setQuery(queryBuilder)
                .setSize(10000).execute().actionGet();
        //1000 hits per shard will be returned for each scroll
        //Scroll until no hits are returned
        while (true) {

            for (SearchHit hit : scrollResp.getHits().getHits()) {
                //Handle the hit...
                list.add(hit.getSourceAsString());
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

       return Long.valueOf(list.size());

    }


    /**
     * 按照ID简单删除
     * @param indexname
     * @param type
     * @param id
     */
    public static void deleteinId(String indexname, String type,String id){
        getTransportClient().prepareDelete(indexname, type, id).execute();
    }

    public static String getRandomString(int length) { //length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }
    

    public static void main(String[] args) {


//  统计查询，查询所有的数据条数。
        BoolQueryBuilder must = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());
        List<String> strings = scrollSearcher(must, "dwtest5", "all");
        Long count = count(must, "dwtest5", "all");

        System.out.println("查询出的所有"+count+"================条数");
        System.out.println(strings);



    }

    /**
     * 新增字段
     * */
    public static void updateIndexMapping(String index,String type,String colname){

        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(type)	//必须是类型名称
                    .startObject("properties")
                    .startObject(colname).field("type","string").field("index","not_analyzed" ).endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }

    public static void createBusLogIndexMapping(){

        String indexname = "performance";
        String typename = "all";
        client.admin().indices().prepareCreate(indexname).execute().actionGet();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(typename)    //必须是类型名称
                    .startObject("properties")
                     .startObject("id").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("batchid").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpiCnName").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpiEnName").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kbpClass").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("ip").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("appSystem").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("grouptext").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    
    /**
     * 		性能数据es表的建立
     */
    
    public static void createPerformanceIndexMapping(){

        String indexname = "performance";
        String typename = "all";
        client.admin().indices().prepareCreate(indexname).execute().actionGet();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(typename)    //必须是类型名称
                    .startObject("properties")
                     .startObject("id").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("batchid").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpiCnName").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpiEnName").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kbpClass").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("ip").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("appSystem").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("groupText").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void createForecasResultMapping(){

        String indexname = "forecastresult";
        String typename = "all";
        client.admin().indices().prepareCreate(indexname) .setSettings(Settings.builder()             
                .put("index.number_of_shards", 12)
                .put("index.number_of_replicas", 1)
        		).get();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(typename)    //必须是类型名称
                    .startObject("properties")
                    .startObject("ip").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpiCnName").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("appSystem").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("groupText").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kbpNo").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
    public static void createcourselogMapping(){

        String indexname = "topay_courselog";
        String typename = "all";
        client.admin().indices().prepareCreate(indexname) .setSettings(Settings.builder()             
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
        		).get();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(typename)    //必须是类型名称
                    .startObject("properties")
                    .startObject("CransactionID").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("StartTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("EndTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("elapsedTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("TransactionID").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("probeType").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("Code").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
    public static void createbuslogMapping(){

        String indexname = "topay_buslog";
        String typename = "all";
        client.admin().indices().prepareCreate(indexname) .setSettings(Settings.builder()             
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
        		).get();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(typename)    //必须是类型名称
                    .startObject("properties")
                    .startObject("TransactionID").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("busname").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("IDValue").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("CBOSSU_StartTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("CBOSSU_EndTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("CBOSSU_elapsedTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("CBOSSU_Code").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("REMOTEU_StartTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("REMOTEU_EndTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("REMOTEU_elapsedTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("REMOTEU_Code").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("APPU_StartTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("APPU_EndTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("APPU_elapsedTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("APPU_Code").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("ZG_StartTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("ZG_EndTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("ZG_elapsedTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("ZG_Code").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("APPD_StartTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("APPD_EndTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("APPD_elapsedTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("APPD_Code").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("REMOTED_StartTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("REMOTED_EndTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("REMOTED_elapsedTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("REMOTED_Code").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("CBOSSD_StartTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("CBOSSD_EndTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("CBOSSD_elapsedTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("CBOSSD_Code").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("hour_interval").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("day_interval").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("week_interval").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("month_interval").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
    /**
     * 新增加根据条件拼接id串，批量删除数据方法
     * @param indexname
     * @param type
     * @param qb
     */
    public static void deleteByQueryNew(String indexname,String type,QueryBuilder qb){
    	//创建bulk，用来存放根据id删除数据的request
    	BulkRequestBuilder prepareBulk = client.prepareBulk();
    	
    	//根据传递条件轮询结果集，将id得到后拼接到bulk中
    	SearchResponse scrollResp = client.prepareSearch(indexname).setTypes(type)
//		        .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
		        .addSort(SortBuilders.scoreSort())
		        .setScroll(new TimeValue(60000))
		        .setQuery(qb)
		        .setSize(10000).execute().actionGet(); //1000 hits per shard will be returned for each scroll
		//Scroll until no hits are returned
		while (true) {

		    for (SearchHit hit : scrollResp.getHits().getHits()) {
		        //Handle the hit...
		    	String id = hit.getId();
		    	prepareBulk.add(client.prepareDelete(indexname,type,id).request());
		    }
		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
		    //Break condition: No hits are returned
		    if (scrollResp.getHits().getHits().length == 0) {
		        break;
		    }
		}
		
		//执行批量删除
		BulkResponse bulkResponse = prepareBulk.get();
		if (bulkResponse.hasFailures()) {
			for(BulkItemResponse item : bulkResponse.getItems()){
				System.out.println(item.getFailureMessage());
			}
		}else {
			System.out.println("delete finished");
		}
    }
    
    public static int  queryBySize(List<String> list,QueryBuilder queryBuilder, String indexname, String type,int from ,int size){     

        //查询那个索引的那个类型
        SearchResponse searchResponse = getTransportClient().prepareSearch(indexname).setTypes(type)
        .setQuery(queryBuilder)
        .setFrom(from)
        .setSize(size)
        .addSort("_id", SortOrder.ASC)
        .execute()
        .actionGet();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for(int i=0;i<searchHists.length;i++){
            list.add(searchHists[i].getSourceAsString());
        }
        return searchHists.length;
    }
    
    public static void createPmQuarterHourMapping(){

        String indexname = "performance_quarter_hour";
        String typename = "all";
        client.admin().indices().prepareCreate(indexname).execute().actionGet();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(typename)    //必须是类型名称
                    .startObject("properties")
                    .startObject("kpiCnName").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpiEnName").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kbpClass").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("ip").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("appSystem").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kbp").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kbpNo").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("grouptext").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void createAlert(){

        String indexname = "alert";
        String typename = "all";
        client.admin().indices().prepareCreate(indexname).execute().actionGet();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(typename)    //必须是类型名称
                    .startObject("properties")
                    .startObject("eventid").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("title").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("text").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("severity").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("moname").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("parentnode").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("source").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpi_no").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpi_id").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("kpi_cn_name").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("parentnodeip").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("appsys").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("descr").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void createEVCHANGE(String indexname){

        String typename = "all";
        client.admin().indices().prepareCreate(indexname).execute().actionGet();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject(typename)    //必须是类型名称
                    .startObject("properties")
                    .startObject("eventid").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("role").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("fullname").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("appsys").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("priority").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("nature").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("source").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("influence").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("stutas").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("issolve").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("istimely").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("issuccess").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("moname").field("type", "string").field("index", "not_analyzed").endObject()
      			    .startObject("ip").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("descr").field("type", "string").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(mapping.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
            client.admin().indices().putMapping(mappingRequest).actionGet();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
    public static void createALLOCA(){
    
    String typename = "all";
    client.admin().indices().prepareCreate("alloca").execute().actionGet();
    XContentBuilder mapping;
    try {
        mapping = jsonBuilder().startObject()
                .startObject(typename)    //必须是类型名称
                .startObject("properties")
                .startObject("eventid").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("fullname").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("appsys").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("stutas").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("moname").field("type", "string").field("index", "not_analyzed").endObject()
  			    .startObject("ip").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("descr").field("type", "string").field("index", "not_analyzed").endObject()
                .endObject()
                .endObject()
                .endObject();
        System.out.println(mapping.toString());
        PutMappingRequest mappingRequest = Requests.putMappingRequest("alloca").type(typename).source(mapping);
        client.admin().indices().putMapping(mappingRequest).actionGet();
    }catch (Exception e){
        e.printStackTrace();
    }
}


    public static List<String>  getAllCourse(String indexName){

        TransportClient client = ElasticSearchHandler.getTransportClient();

        List<String> allCourse =null;
        IndicesExistsRequest existsRequest = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(existsRequest).actionGet();
        if (response.isExists()){

            ImmutableOpenMap<String, MappingMetaData> mappings = client.admin().cluster().prepareState().execute()
                    .actionGet().getState().getMetaData().getIndices().get(indexName).getMappings();
            String s = mappings.get("all").get().source().toString();

            JSONObject jsonObject = JSON.parseObject(s);
            JSONObject jsonObject1 = JSON.parseObject(jsonObject.get("all").toString());
            JSONObject keys = JSON.parseObject(jsonObject1.get("properties").toString());
//       获取到所有字段。
            Set<String> strings = keys.keySet();
            allCourse = strings.stream().filter(line -> line.contains("_") && line.contains("success")).map(s2 -> s2.split("_")[0]).collect(Collectors.toList());
        }

        return  allCourse;
    }

    public static Boolean ifIndexExits(String index){

        IndicesExistsRequest existsRequest = new IndicesExistsRequest(index);
        IndicesExistsResponse response = client.admin().indices().exists(existsRequest).actionGet();

        return response.isExists();
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
            object.put("dcTime",startTime);
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
                        object.put("notTimeOutNum", docCount);
                    } else {
                        object.put("timeOutNum", docCount);
                    }

                }
            }

            InternalFilters avgTimefilter = (InternalFilters) bucket.getAggregations().get("timefilter4");

            for (InternalFilters.InternalBucket bu : avgTimefilter.getBuckets()) {
                Double avgTime = ((InternalAvg) bu.getAggregations().get("avgTime")).getValue();
                if (!avgTime.isNaN()){
                    object.put("avgTime", avgTime);
                }
            }

            jsonObjects.add(object);
        }

        return jsonObjects;
    }

    /**
     *
     * 汇聚原始粒度的索引数据。
     * @param indexName 查询的索引名
     * @param timeColume    查询的时间的列
     * @param startTime     查询时间段 开始时间
     * @param endTime       查询时间段：结束时间
     * @param busTypeColume     业务类型列
     * @param courseTypeColume  环节类型列
     * @param countNumColume    统计出的总量列
     * @param successColume     统计出的成功量列
     * @param failNumColume     统计出的失败量列
     * @param timeOutNumColume  统计出的超时量列
     * @param avgTimeColume     统计出的平均耗时列
     * @return 返回为 json list 格式为：[{"courseType":"all","failNum":4,"countNum":1656,"successNum":1652,"timeOutNum":122,"busType":"套餐变更"}]
     *
     */
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



}
