package com.wlqq.bigdata.es;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.wlqq.bigdata.monitor.Monitor;
import com.wlqq.bigdata.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class FaultTolerantBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(FaultTolerantBolt.class);

	private Map<String, Object> userConfig;
	private Monitor monitor;
	private String monitorTopic;
	private Thread executor = null;
	Map<String,Map<String,Object>> map = new HashMap<String, Map<String,Object>>();//存放监控的指标值
	
	public FaultTolerantBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
	
	public void execute(Tuple tuple) {
		String streamID = tuple.getSourceStreamId();
		if (!Utils.SUCCESS_STREAM.equals(streamID)) {
			Result res = (Result) tuple.getValueByField(Utils.OUTPUT_STREAM_FIELDS_NAME);
			logger.info("###Failed result,StreamID:"+streamID+",result:" + res.toString());
			
			//写入monitor 的topic里面，再写入es
			/*switch (res.getStatus()) {
			case FAILED_RAWDATA_FORMAT_ERROR: {
				int num = (Integer) map.get("FAILED_RAWDATA_FORMAT_ERROR").get("num");
				map.get("FAILED_RAWDATA_FORMAT_ERROR").put("num", num+1);
				break;
			}
			case FAILED_BY_ES_RESULT: {
				int num = (Integer) map.get("FAILED_BY_ES_RESULT").get("num");
				map.get("FAILED_BY_ES_RESULT").put("num", num+1);
				break;
			}
			case FAILED_BY_ES_EXECUTE_EXCEPTION: {
				int num = (Integer) map.get("FAILED_BY_ES_EXECUTE_EXCEPTION").get("num");
				map.get("FAILED_BY_ES_EXECUTE_EXCEPTION").put("num", num+1);
				break;
			}
			case FAILED_RECOVERABLE: {
				int num = (Integer) map.get("FAILED_RECOVERABLE").get("num");
				map.get("FAILED_RECOVERABLE").put("num", num+1);
				break;
			}
			case FAILED_REJECT_ERROR: {
				int num = (Integer) map.get("FAILED_REJECT_ERROR").get("num");
				map.get("FAILED_REJECT_ERROR").put("num", num+1);
				break;
			}
			case FAILED_MAPPING_ERROR: {
				int num = (Integer) map.get("FAILED_MAPPING_ERROR").get("num");
				map.get("FAILED_MAPPING_ERROR").put("num", num+1);
				break;
			}
			default: {
				break;
			}
			}*/
			
		}
	}
	

	public void initMap(Map<String,Map<String,Object>> map){
		
		map.put("FAILED_RAWDATA_FORMAT_ERROR", new HashMap<String,Object>(){
			{
				put("business", userConfig.get(Utils.TOPOLOGY_NAME).toString());
				put("topic", userConfig.get(Utils.TOPIC).toString());
				put("num",0);
				put("message", "FAILED_RAWDATA_FORMAT_ERROR");
			}
			
		});
		
		map.put("FAILED_BY_ES_RESULT", new HashMap<String,Object>(){
			{
				put("business", userConfig.get(Utils.TOPOLOGY_NAME).toString());
				put("topic", userConfig.get(Utils.TOPIC).toString());
				put("num",0);
				put("message", "FAILED_BY_ES_RESULT");
			}
			
		});
		
		map.put("FAILED_BY_ES_EXECUTE_EXCEPTION", new HashMap<String,Object>(){
			{
				put("business", userConfig.get(Utils.TOPOLOGY_NAME).toString());
				put("topic", userConfig.get(Utils.TOPIC).toString());
				put("num",0);
				put("message", "FAILED_BY_ES_EXECUTE_EXCEPTION");
			}
			
		});
		
		map.put("FAILED_RECOVERABLE", new HashMap<String,Object>(){
			{
				put("business", userConfig.get(Utils.TOPOLOGY_NAME).toString());
				put("topic", userConfig.get(Utils.TOPIC).toString());
				put("num",0);
				put("message", "FAILED_RECOVERABLE");
			}
			
		});
		
		map.put("FAILED_REJECT_ERROR", new HashMap<String,Object>(){
			{
				put("business", userConfig.get(Utils.TOPOLOGY_NAME).toString());
				put("topic", userConfig.get(Utils.TOPIC).toString());
				put("num",0);
				put("message", "FAILED_REJECT_ERROR");
			}
			
		});
		
		map.put("FAILED_MAPPING_ERROR", new HashMap<String,Object>(){
			{
				put("business", userConfig.get(Utils.TOPOLOGY_NAME).toString());
				put("topic", userConfig.get(Utils.TOPIC).toString());
				put("num",0);
				put("message", "FAILED_MAPPING_ERROR");
			}
			
		});
	}
	
	public void setZero(Map<String,Map<String,Object>> map){
		map.get("FAILED_RAWDATA_FORMAT_ERROR").put("num",0);
		map.get("FAILED_BY_ES_RESULT").put("num",0);
		map.get("FAILED_BY_ES_EXECUTE_EXCEPTION").put("num",0);
		map.get("FAILED_RECOVERABLE").put("num",0);
		map.get("FAILED_REJECT_ERROR").put("num",0);
		map.get("FAILED_MAPPING_ERROR").put("num",0);
	}
	
	public void writeToKafka(Map<String,Map<String,Object>> map){
		for (Entry<String, Map<String,Object>> entry : map.entrySet()) {
			if(((Integer) entry.getValue().get("num")) != 0){
				String infoJson = monitor.getJson(entry.getValue());
				entry.getValue().put("num", 0);//重设0
				monitor.produce(monitorTopic, null, infoJson);
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		
//		monitor = new Monitor(userConfig);
//    	monitorTopic = Utils.getValue(userConfig, Utils.MONITOR_TOPIC, "monitor");
//		initMap(map);
//
//		executor = new Thread(new Runnable() {
//			public void run() {
//				logger.info("thread start...");
//				while(true){
//					writeToKafka(map);
//					try {
//						Thread.sleep(1000);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//				
//			}
//	     });
//	    executor.start();
    }
		
	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}
}
