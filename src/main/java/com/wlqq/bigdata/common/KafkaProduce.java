package com.wlqq.bigdata.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import storm.kafka.KafkaUtils;

public class KafkaProduce implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static final Log logger = LogFactory.getLog(KafkaProduce.class);
	KafkaProducer<String, String> producer;
	Properties props = new Properties();
	Map<String, Object> userConfig;
	
	public KafkaProduce(Map<String, Object> userConfig){
		
		this.userConfig = userConfig;
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
		    		Utils.getValue(userConfig, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "v32:6667,v29:6667"));
		props.put(ProducerConfig.ACKS_CONFIG,
		    		Utils.getValue(userConfig, ProducerConfig.ACKS_CONFIG, "1"));
		props.put(ProducerConfig.RETRIES_CONFIG,
		    		Utils.getValue(userConfig, ProducerConfig.RETRIES_CONFIG, 0));
		props.put(ProducerConfig.BATCH_SIZE_CONFIG,
		    		Utils.getValue(userConfig, ProducerConfig.BATCH_SIZE_CONFIG, 16384));
		props.put(ProducerConfig.LINGER_MS_CONFIG,
		    		Utils.getValue(userConfig, ProducerConfig.LINGER_MS_CONFIG, 0));
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
		    		Utils.getValue(userConfig, ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
		    		Utils.getValue(userConfig, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
		    		Utils.getValue(userConfig, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"));
		//props.put("producer.type", "async");
		producer = new KafkaProducer(props);
	}
	
	public void produce(String topic,String key,String json){
		produce(topic,key,json,null,null);
	}
	
	public void produce(String topic,String key,String json,final OutputCollector collector,final Tuple input){
		
		final ProducerRecord<String,String> record;
		if(key==null || "".equals(key)){//轮训方式
			record = new ProducerRecord(topic, json);
		}else{
			record = new ProducerRecord(topic,key, json);//按照key进行hash
		}
    	
    	producer.send(record,
                new Callback() {
                     public void onCompletion(RecordMetadata metadata, Exception e) {
                         if(e != null){
                        	 logger.error(e);
                        	 if(collector!=null){//send again
                        		 collector.fail(input);
                        	 }else{//save fail record
                        		//save fail record
                        	 }
                         }else{
                        	 if(collector!=null){
                        		 collector.ack(input);
                        	 }
                        	 //collector.ack(input);
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
