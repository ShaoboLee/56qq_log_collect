package com.wlqq.bigdata.kafka;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class EScheme implements Scheme {
	
	long count = 0;
	long total = 0l;
	long begin;
	boolean flag = true;
	
	private static final Log logger = LogFactory.getLog(EScheme.class);

	public List<Object> deserialize(byte[] ser) {
		
		if(flag){
			begin = System.currentTimeMillis();
			flag = false;
		}
		
		long start = System.currentTimeMillis();
		
		List<Object> value = new ArrayList<Object>();
		String json = new String(ser);
		String key = null;
		try{
			
			JSONObject jo = JSONObject.parseObject(json);
			String host = jo.getJSONObject("file_location").getString("host");
			String service = jo.getJSONObject("file_location").getString("service");
			String docker = jo.getJSONObject("file_location").getString("docker");
			key = host+"-"+service+"-"+docker;
		    //logger.info("sequence_number="+jo.getString("sequence_number"));
		}catch(Exception e) {
			logger.error(e);
		}
		
		value.add(json);
		value.add(key==null?"":key);
		count++;
		long end = System.currentTimeMillis();
		total +=(end-start);
		
		if(count%10000==0){
			System.out.println("spout parse json "+count+"Ìõ,cost time:"+total/1000+"s,"+"-----"+(System.currentTimeMillis()-begin)/1000+"s");
		}
		return value;
	}

	public Fields getOutputFields() {
		return new Fields("json", "key");
	}

}
