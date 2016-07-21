package com.wlqq.bigdata.utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.alibaba.fastjson.JSONObject;
//import com.wlqq.bigdata.monitor.Monitor;
import com.wlqq.bigdata.utils.StorageFailRecord;
import com.wlqq.bigdata.utils.StorageToLocalDisk;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class KafkaProduce implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static final Log logger = LogFactory.getLog(KafkaProduce.class);
	KafkaProducer<String, String> producer;
	Properties props = new Properties();
	Map<String, Object> userConfig;
    String path;
    String newLine; 
    StorageFailRecord sfr;
    int totalFailCount = 0;
    int continuityFailCount = 0;
	long lastFailUnixtime = 0l;
	OutputCollector collector;
	
	private Thread executor = null;
	Map<String,Long> map;
	//Monitor monitor;
    
	public KafkaProduce(Map<String, Object> userConfig,OutputCollector collector){
		
		this.collector = collector;
		newLine = System.getProperty("line.separator");
		this.userConfig = userConfig;
		props.putAll(userConfig);
		producer = new KafkaProducer(props);
		path = Utils.getValue(userConfig, Utils.FAIL_RECORD_STORAGE, "");
		String className = Utils.getValue(userConfig, Utils.FAIL_RECORD_DEAL_CLASS, "");
		if(!"".equals(path) && className.equals("com.wlqq.bigdata.common.StorageToLocalDisk")){
			sfr = new StorageToLocalDisk(path);
		}
		
		map = new HashMap<String, Long>();
		//monitor = new Monitor(userConfig);
		try {
			init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Init elasticsearch failed.", e);
		}
	}
	
	public void init() throws IOException{
		//monitor.initES();
		executor = new Thread(new Runnable() {
			Map<String,String> loadMap = new HashMap<String, String>();
			public void run() {
				while(true){
					if(map.isEmpty()){
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}else{
						loadMap.clear();
						for (Entry<String, Long> entry : map.entrySet()) {
							long num = entry.getValue();
							if(num!=0){
								map.put(entry.getKey(), 0l);
								Map<String,Object> m1 = new HashMap<String, Object>();
								m1.put("business", userConfig.get(Utils.TOPOLOGY_NAME).toString());
								m1.put("topic", entry.getKey());
								m1.put("message", "write to kafka fail");
								m1.put("num", num);
								String id = entry.getKey()+"-"+System.currentTimeMillis();
								m1.put("id", id);
								//loadMap.put(id, monitor.getJson(m1));
							}
						}
						//monitor.bulkLoadDocuments(loadMap);
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

	public int getTotalFailCount(){
		return totalFailCount;
	}
	
	public int getContinuityFailCount(){
		return continuityFailCount;
	}
	
	public void setContinuityFailCount(int continuityFailCount){
		this.continuityFailCount = continuityFailCount;
	}
	
	/**
	 * 异步发送数据到kafka，由于collector的ack、fail、emit方法需要在同一个线程里面执行，
	 * 所以都放到了异步里面处理了，当连续出现大量的写入失败，这时会有两个动作：
	 * 1）暂时停止这个topology的执行，调用了 deactivate
	 * 2）把此时失败的记录调用fail方法
	 * @param topic
	 * @param key
	 * @param json
	 * @param input
	 */
	public void produce(final String topic,String key,final String json,final Tuple input){
		
		final ProducerRecord<String,String> record;
		if(key==null || "".equals(key)){//轮训方式
			record = new ProducerRecord(topic, json);
		}else{
			record = new ProducerRecord(topic,key, json);//按照key进行hash
		}
    	
    	producer.send(record,
                new Callback() {
                     public void onCompletion(RecordMetadata metadata, Exception e) {
                         if(e != null){//fail
                        	 
                        	//记录到es
                        	 if(!map.containsKey(topic)){
                        		 map.put(topic, 1l);
                        	 }else{
                        		 map.put(topic, map.get(topic)+1);
                        	 }
                        	 
                        	 String message = "write to kafka fail";
                        	 totalFailCount++;
                        	 if(System.currentTimeMillis()-lastFailUnixtime<10*1000){//连续失败的记录前后不能超过10秒时间，否则清0
                        		 lastFailUnixtime = System.currentTimeMillis();
                        		 continuityFailCount++;
                        	 }else{
                        		 lastFailUnixtime = System.currentTimeMillis();
                        		 continuityFailCount = 1;
                        	 }
                        	 if(sfr!=null){
                        		 JSONObject jb = new JSONObject();
                             	 jb.put(topic, JSONObject.parse(json));
                        		 sfr.storage(jb.toString(),topic,e,message);
                        		 logger.error("topic="+topic, e);
                        	 }else{
                        		 logger.error("topic="+topic+",json="+json, e);
                        	 }
                        	collector.emit(Utils.KAFKA_WRITE_FAIL_STREAM,new Values(topic,message,e.toString(),json));
                        	if(continuityFailCount>=Utils.getValue(userConfig, Utils.KAFKA_WRITE_FAIL_THRESHOLD, 5)){//连续的失败，通过fail进行重试
                        		//是否调用ack，当连续出现写入kafka失败时，可以考虑调用fail来减少数据丢失的 问题，但同时带来了重复数据
                        		if(Utils.getValue(userConfig, Utils.STORM_KAFKA_COLLECTOR_ACK, true)){
                        			collector.ack(input);
                        		}else{
                        			collector.fail(input);
                        		}
                        	}else{
                        		collector.ack(input);
                        	}
                         }else{//success
                        	 continuityFailCount = 0;
                        	 collector.emit(Utils.SUCCESS_STREAM,new Values("","","",""));
                        	 collector.ack(input);
                         }
            
                     }
                });
    }
	
    public static void main(String[] args){
		
		String json = "{\"logs\":{" +
				"\"1001\":["+"{\"common\":{\"_uid_\":1111,\"b\":100},\"data\":{\"count\":5}}],"+
				"\"1003\":["+"{\"common\":{\"_uid_\":3333,\"d\":300},\"data\":{\"count\":55}}],"+
				"\"1002\":["+"{\"common\":{\"_uid_\":2222,\"c\":200},\"data\":{\"count\":50}}]}}";
//		String json = "{\"logs\":{\"1001\":[{\"common\":{\"a\":1,\"_uid_\":1111},{\"b\":100}}]," +
//				"\"1002\":[{\"common\":{\"c\":1,\"_uid_\":2222},{\"d\":55}]}}";
//		KafkaProduce p = new KafkaProduce(new HashMap<String, Object>());
//		int i=0;
//		while(i<10){
//			p.produce("1001","1",json,null,null);
//			i++;
//		}
		
		if(1==2){
			System.out.println("false");
		}else{
			System.out.println("true");
		}
		
/*		String[] options = new String[]{  
			    "--list",  
			    "--zookeeper",  
			    "v32:2181,v29:2181,v30:2181"  
			}; 
		//TopicCommand.main(options);
		ZkClient zc = new ZkClient("v32:2181,v29:2181,v30:2181");
		System.out.println(ZkUtils.getAllTopics(zc));
		System.out.println(kafka.api.OffsetRequest.LatestTime());*/
		
    }
	
	
	
	

}
