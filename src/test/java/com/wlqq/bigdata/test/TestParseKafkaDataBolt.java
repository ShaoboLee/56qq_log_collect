package com.wlqq.bigdata.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import backtype.storm.task.OutputCollector;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wlqq.bigdata.commons.KafkaProduceAndES;

public class TestParseKafkaDataBolt {
	
	private static final Log logger = LogFactory.getLog(TestParseKafkaDataBolt.class);
    private static final long serialVersionUID = 886149197481637894L;
	JSONObject jb1,jb2,jb3;
	ProducerConfig config;
	OutputCollector collector;
	KafkaProduceAndES p;
	Map<String, Object> userConfig;
	
	String str;
    public  void readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                System.out.println("line " + line + ": " + tempString);
                str = tempString;
                break;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
    
    public void test(String json){
        
        try{
        	
        	Time time = new SystemTime();
    		long begin = time.milliseconds();
			jb1 = JSONObject.parseObject(json); 
			long end = time.milliseconds();
			System.out.println("JSONObject.parseObject cost "+(end-begin)+"ms");
			
        }catch(Exception e){
			logger.error("parse json fail, json="+json,e);
			//p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json,collector,input);
			//collector.ack(input);
			return;
        }
        
        Set<String> sets;//log task id
        
        try{
       	 jb2 = jb1.getJSONObject("logs");
       	 sets = jb2.keySet();
        }catch (Exception ex) {//"logs is not exists"
       	 logger.error("loss field logs, json="+json,ex);
       	 //p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json,collector,input);
       	 //collector.ack(input);
       	 return;
        }
        
        String _terminal_ = jb1.getString("_terminal_");
        int i = 0;
        
        for(String id:sets){
       	 
				JSONArray ja = jb2.getJSONArray(id);
				Object[] obs = ja.toArray();
				
				for(Object ob:obs){
					try{
						jb3 = JSONObject.parseObject(ob.toString());
						String dfp_ = jb3.getJSONObject("common").getString("_dfp_").toString();
						if(_terminal_!=null){//add terminal
							((JSONObject)ob).getJSONObject("common").put("_terminal_", _terminal_);
						}
						//collector.emit(input,new Values(id,dfp_,ob.toString()));
					}catch (Exception ex) {//get _dfp_ fail
						logger.error("loss field _dfp_, json="+json,ex);
			            if(i==0){//send one time 
			            	//p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json);
			            }
			            i++;
			        }
				}
        }
    }
    
    public static void main(String[] args){
    	TestParseKafkaDataBolt pkdb = new TestParseKafkaDataBolt();
    	pkdb.readFileByLines("d:\\one.txt");
    	
    	Time time = new SystemTime();
    	System.out.println(pkdb.str);
		long begin = time.milliseconds();
		pkdb.test(pkdb.str);
		long end = time.milliseconds();
		System.out.println("cost "+(end-begin)+"ms");
    }

}
