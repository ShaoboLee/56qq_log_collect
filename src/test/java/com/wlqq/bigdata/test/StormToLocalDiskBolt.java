package com.wlqq.bigdata.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class StormToLocalDiskBolt extends BaseRichBolt {

	private static final Log logger = LogFactory.getLog(StormToLocalDiskBolt.class);
    private static final long serialVersionUID = 886149197481637891L;
    Map<String, Object> userConfig;
    OutputCollector collector;
    String path;
    String newLine; 

	public StormToLocalDiskBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
		newLine = System.getProperty("line.separator");
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		//this.path = Utils.getValue(userConfig, "", "");
		this.path = "/tmp/data/fail_record.txt";
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
    	String topic = input.getStringByField("topic");
    	String json = input.getStringByField("json");
    	JSONObject jb = new JSONObject();
    	jb.put(topic, JSONObject.parse(json));
    	json = jb.toJSONString();
    	writeToLocalDisk(json);
    	collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	public void writeToLocalDisk(String json){
		
		String directory = path.substring(0,path.lastIndexOf(File.separator));
		File dir = new File(directory);
		
		if(!dir.exists()){
			dir.mkdirs();
		}
		
		File file=new File(path);
		FileOutputStream out;
		
        if(!file.exists()){
        	try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        try {
			out=new FileOutputStream(file,true);
			out.write((json+newLine).getBytes());
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	public static void main(String[] args){
		StormToLocalDiskBolt stld = new StormToLocalDiskBolt(null);
		stld.path = "d:\\1\\2\\3\\test.txt";
		stld.writeToLocalDisk("this wowŒ“√«is a test");
		stld.writeToLocalDisk("this is anther test");
	}

}
