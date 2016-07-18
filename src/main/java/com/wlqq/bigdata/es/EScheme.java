package com.wlqq.bigdata.es;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class EScheme implements Scheme {
	
	private static final Log logger = LogFactory.getLog(EScheme.class);

	public List<Object> deserialize(byte[] ser) {
		
		List<Object> value = new ArrayList<Object>();
		String json = new String(ser);
		String key = null;
		String tmp = json;

		try{
			
			JSONObject jo = JSONObject.parseObject(json);
			String host = jo.getJSONObject("file_location").getString("host");
			String service = jo.getJSONObject("file_location").getString("service");
			String docker = jo.getJSONObject("file_location").getString("docker");
			key = host+"-"+service+"-"+docker;
			String method = jo.getString("method");
			
		}catch(Exception e) {
			logger.error(e);
		}
		
		value.add(json);
		value.add(key==null?"":key);
		return value;
	}
	
	public Fields getOutputFields() {
		return new Fields("json", "key");
	}

}
