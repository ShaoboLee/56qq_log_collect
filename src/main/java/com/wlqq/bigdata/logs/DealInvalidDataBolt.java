package com.wlqq.bigdata.logs;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
import com.wlqq.bigdata.monitor.Monitor;
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
	Monitor monitor;
	String monitorTopic;
	Client client;
	Map<String,Map<String,Object>> map;//存放监控的指标值
	private Thread executor = null;
	
	public DealInvalidDataBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
	 
    public void prepare(Map stormConf, TopologyContext context,
              OutputCollector collector) {
    	
    	map = new HashMap<String, Map<String,Object>>();
    	client = NimbusClient.getConfiguredClient(stormConf).getClient();
    	monitor = new Monitor(userConfig);
    	monitorTopic = Utils.getValue(userConfig, Utils.MONITOR_TOPIC, "monitor");
	    zc = new ZkClient(userConfig.get(Utils.ZKS).toString());
	    this.collector = collector;
	    begin = System.currentTimeMillis();
	    updateTopicsInfoIntervalMs = Utils.getValue(userConfig, Utils.UPDATE_TOPICS_INFO_INTERVAL_Ms, 60000l);
	    topics = new HashMap<String,String>();
	    updateTopicInfo();
	    
	    executor = new Thread(new Runnable() {
			public void run() {
				logger.info("thread start...");
				while(true){
					if(map.isEmpty()){
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}else{
						for (Entry<String, Map<String,Object>> entry : map.entrySet()) {
							if(((Integer) entry.getValue().get("num")) != 0){
								Map<String,Object> m1 = new HashMap<String, Object>(entry.getValue());
								m1.put("topic", entry.getKey());
								String infoJson = monitor.getJson(m1);
					    		monitor.produce(monitorTopic, null, infoJson);
					    		entry.getValue().put("num", 0);//重新置0
							}
						}
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				
			}
	     });
	    executor.start();
    }

    /**
     * 判断topic是否存在，有就把json发送到这个topic，没有就发送到DEFAULT_UNKNOWN_TOPIC（同时把这个未知的topic也加入到了json里面）
     */
    public void execute(Tuple input) {
    	
    	String topic = input.getStringByField("topic");
    	String _dfp_ = input.getStringByField("_dfp_");
    	String json = input.getStringByField("json");
    	String message = input.getStringByField("message");
    	String exception = input.getStringByField("exception");
    	
    	if(exception!=null && !"".equals(exception)){
    		//监控异常数据
    		String infoJson = monitor.getJson(userConfig.get(Utils.TOPOLOGY_NAME).toString(), 
    				userConfig.get(Utils.TOPIC).toString(), exception, message, json);
    		monitor.produce(monitorTopic, null, infoJson);
    		collector.emit(Utils.RAWDATA_FORMAT_ERROR_STREAM,new Values(Utils.getValue(userConfig, Utils.TOPIC, "wrong-data-topic"),message,exception,json));
    	}
    	
    	if(System.currentTimeMillis()-begin>=updateTopicsInfoIntervalMs){//update topics
    		updateTopicInfo();
    		begin = System.currentTimeMillis();
    	}
    	
    	if(!topics.containsKey(topic)){//出现不存在的topic
//    		monitor.produce(monitorTopic, null, 
//    				monitor.getJson(topic, null, "topic:"+topic+" is not exist",json));
    		//logger.error("unknown-topic:"+topic+"is not exists");
    		if(map.isEmpty() || !map.containsKey(topic)){//topic还没有存放
    			Map<String,Object> m = new HashMap<String,Object>();
    			m.put("business", userConfig.get(Utils.TOPOLOGY_NAME).toString());
    			m.put("message", "unknown topic");
    			m.put("num", 1);
    			map.put(topic, m);
    		}else{
    			map.get(topic).put("num", ((Integer) map.get(topic).get("num"))+1);
    		}
    		collector.emit(Utils.UNKNOWN_TOPIC_STREAM,new Values(topic,"unknown-topic","",json));
    		JSONObject jb = new JSONObject();
    		jb.put(topic, JSONObject.parse(json));
    		json = jb.toJSONString();
    		topic = Utils.getValue(userConfig, Utils.DEFAULT_UNKNOWN_TOPIC, "unknown-topic");
    		
    	}
    	collector.emit(Utils.KAFKA_WRITE_DATA_STREAM,input,new Values(topic,_dfp_,json));
    	collector.ack(input);
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declareStream(Utils.RAWDATA_FORMAT_ERROR_STREAM, new Fields("topic","message","exception","json"));
		declare.declareStream(Utils.UNKNOWN_TOPIC_STREAM, new Fields("topic","message","exception","json"));
		declare.declareStream(Utils.KAFKA_WRITE_DATA_STREAM, new Fields("topic","_dfp_","json"));
                 
    }
    
    public void updateTopicInfo(){
    	
	    Seq<String> tps = ZkUtils.getAllTopics(zc);//cost 80ms
	    Iterator it =  tps.toIterator();
	    
	    while(it.hasNext()){
	    	topics.put(it.next().toString(), null);
	    }
    }
}
