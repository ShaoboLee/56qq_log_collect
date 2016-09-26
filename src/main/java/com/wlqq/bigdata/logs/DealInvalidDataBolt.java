package com.wlqq.bigdata.logs;

import java.util.HashMap;
import java.util.Map;

import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import scala.collection.Iterator;
import scala.collection.Seq;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;

import com.alibaba.fastjson.JSONObject;
import com.wlqq.bigdata.utils.Utils;

public class DealInvalidDataBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(DealInvalidDataBolt.class);
    private static final long serialVersionUID = 886149197481637891L;
	Map<String, Object> userConfig;
	ZkClient zc;//find topics
	OutputCollector collector;
	HashMap<String,String> topics;
	long begin;
	long updateTopicsInfoIntervalMs;
	Client client;
	
	public DealInvalidDataBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
	 
    public void prepare(Map stormConf, TopologyContext context,
              OutputCollector collector) {
    	
    	client = NimbusClient.getConfiguredClient(stormConf).getClient();
	    zc = new ZkClient(userConfig.get(Utils.ZKS).toString());
	    this.collector = collector;
	    begin = System.currentTimeMillis();
	    updateTopicsInfoIntervalMs = Utils.getValue(userConfig, Utils.UPDATE_TOPICS_INFO_INTERVAL_Ms, 60000l);
	    topics = new HashMap<String,String>();
	    updateTopicInfo();
    }

    /**
     * 判断topic是否存在，有就把json发送到这个topic，没有就发送到DEFAULT_UNKNOWN_TOPIC（同时把这个未知的topic也加入到了json里面）
     */
    public void execute(Tuple input) {
    	
    	String topic = input.getStringByField("topic");
    	String json = input.getStringByField("json");
    	String message = input.getStringByField("message");
    	String exception = input.getStringByField("exception");
    	
    	if(exception!=null && !"".equals(exception)){
    		//监控异常数据
    		collector.emit(Utils.RAWDATA_FORMAT_ERROR_STREAM,new Values(Utils.getValue(userConfig, Utils.TOPIC, "wrong-data-topic"),message,exception,json));
    	}
    	
    	if(System.currentTimeMillis()-begin>=updateTopicsInfoIntervalMs){//update topics
    		updateTopicInfo();
    		begin = System.currentTimeMillis();
    	}
    	
    	if(!topics.containsKey(topic)){//出现不存在的topic
    		collector.emit(Utils.UNKNOWN_TOPIC_STREAM,new Values(topic,"unknown-topic","",json));
    		JSONObject jb = new JSONObject();
    		jb.put(topic, JSONObject.parse(json));
    		json = jb.toJSONString();
    		topic = Utils.getValue(userConfig, Utils.DEFAULT_UNKNOWN_TOPIC, "unknown-topic");
    		
    	}
    	collector.emit(Utils.KAFKA_WRITE_DATA_STREAM,input,new Values(topic,json));
    	collector.ack(input);
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declareStream(Utils.RAWDATA_FORMAT_ERROR_STREAM, new Fields("topic","message","exception","json"));
		declare.declareStream(Utils.UNKNOWN_TOPIC_STREAM, new Fields("topic","message","exception","json"));
		declare.declareStream(Utils.KAFKA_WRITE_DATA_STREAM, new Fields("topic","json"));
                 
    }
    
    public void updateTopicInfo(){
    	
	    Seq<String> tps = ZkUtils.getAllTopics(zc);//cost 80ms
	    Iterator it =  tps.toIterator();
	    
	    while(it.hasNext()){
	    	topics.put(it.next().toString(), null);
	    }
    }
}
