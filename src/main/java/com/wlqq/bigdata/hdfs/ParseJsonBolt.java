package com.wlqq.bigdata.hdfs;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.wlqq.bigdata.common.KafkaProduce;
import com.wlqq.bigdata.common.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 读取指定topic的数据，解析生成文件（指定字段分隔符存放或者就以json格式存放）
 * 
 * @author wangchi
 *
 */
public class ParseJsonBolt  extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(ParseJsonBolt.class);
    private static final long serialVersionUID = 886149197481637894L;
	JSONObject jb,common,data;
	OutputCollector collector;
	Map<String, Object> userConfig;
	KafkaProduce p;
	String delimiter;
	TreeSet<String> ts1;
	TreeSet<String> ts2;
	
	public ParseJsonBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		p = new KafkaProduce(userConfig);
		delimiter = Utils.getValue(userConfig, Utils.HIVE_FIELD_DELIMITER, "\001");
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
        String json = input.getString(0);
        
        try{
			jb = JSONObject.parseObject(json); 
		}catch(Exception e){
			logger.error(e);
			//解析失败的数据，发送到相应的topic
			p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json);
			collector.ack(input);
			return;
		}
        
        common = jb.getJSONObject("common");
        data = jb.getJSONObject("data");
        
        //return json-format
        if(Utils.getValue(userConfig, Utils.HIVE_FILE_FORMAT, "").equals("json")){
        	
        	JSONObject jsb = new JSONObject();
        	
        	if(Utils.getValue(userConfig, Utils.HIVE_FIELD_DELETE_HEAD_UNDERLINE, false)){//删除前下划线
        		for (Map.Entry<String, Object> entry : common.entrySet()) {
        			String k = entry.getKey();
        			jsb.put(k.startsWith("_")?k.substring(1):k, entry.getValue());
                }
        		for (Map.Entry<String, Object> entry : data.entrySet()) {
        			String k = entry.getKey();
        			jsb.put(k.startsWith("_")?k.substring(1):k, entry.getValue());
                }
        	}else{
        		jsb.putAll(common);
        		jsb.putAll(data);
        	}

    		collector.emit(input,new Values(jsb.toString()));
        	collector.ack(input);
        	return;
        }
        
        if(ts1==null){//初始化一次
        	ts1 = new TreeSet(common.keySet());
        	ts2 = new TreeSet(data.keySet());
        }
        
        StringBuffer sb = new StringBuffer();
        
        for(String s:ts1){
        	sb.append(common.getString(s)).append(delimiter);
        }
        
        for(String s:ts2){
        	sb.append(data.getString(s)).append(delimiter);
        }
        
        sb.deleteCharAt(sb.length()-1);
        
        collector.emit(input,new Values(sb.toString()));
    	collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("record"));
		
	}
	
	public static void main(String[] args){
		
		String json ="{\"data\":{\"page_id\":1,\"view_count\":4},\"common\":{\"aa_sid_\":123456,\"_time_\":\"2016-02-18 18:43:52\",\"_vn_\":\"v1.0.1\",\"_uuid_\":\"defb-adfa-eera-2419\",\"_t_\":1455792232,\"_un_\":\"wu_4838\",\"_pn_\":\"com.wlqq.driver\",\"_m_\":\"iphone 10\",\"_lat_\":1.234,\"_no_\":\"china mobile\",\"_lng_\":1.234,\"_ov_\":\"1.0.1\",\"_num_\":273738,\"_did_\":1,\"_ch_\":\"xiaomi3\",\"_dfp_\":\"vfNs-qlsw-7fPE-jrnQ\",\"_nw_\":\"4g\",\"_uid_\":35125}}";
		JSONObject jb = null;
		try{
			jb = JSONObject.parseObject(json); 
		}catch(Exception e){
			logger.error("parse json fail, json="+json,e);
			//解析失败的数据，发送到相应的topic
		}
        
		JSONObject common = jb.getJSONObject("common");
		JSONObject data = jb.getJSONObject("data");
		
		
		JSONObject jsb = new JSONObject();
		
		for (Map.Entry<String, Object> entry : common.entrySet()) {
			String k = entry.getKey();
			jsb.put(k.startsWith("_")?k.substring(1):k, entry.getValue());
        }
		System.out.println(jsb);
//		common.entrySet();
//		
//		
//		jsb.putAll(common);
//		jsb.putAll(data);
//		System.out.println(jsb.toString());
//        
//		TreeSet<String> ts1=null;
//		TreeSet<String> ts2=null;
//		
//        if(ts1==null){//初始化一次
//        	ts1 = new TreeSet(common.keySet());
//        	ts2 = new TreeSet(data.keySet());
//        }
//        
//        
//        StringBuffer sb = new StringBuffer();
//        
//        System.out.println(ts1);
//        for(String s:ts1){
//        	System.out.println(s);
//        	sb.append(common.getString(s)).append("\t");
//        }
//        
//        for(String s:ts2){
//        	sb.append(data.getString(s)).append("\t");
//        }
//		System.out.println(sb.toString());
//		String b = "414";
//		long a = Long.parseLong(common.getString("_num_"));
//		System.out.println(a);
//		if(a<200){
//			System.out.println("true");
//		}
//		Set<String> s = new HashSet<String>();
//		s.add("a");
//		s.add("e");
//		s.add("b");
//		s.add("b");
//		s.add("b");
//		s.add("i");
//		s.add("d");
//		TreeSet<String> ts = new TreeSet(s);
//		System.out.println(ts);
//		for(String str:ts){
//			System.out.println(str);
//		}
//		System.out.println("a\001bc");
//		StringBuffer sb = new StringBuffer();
//		
//		sb.append("我们").append("\001").append("顶顶顶顶").append("\001").append("哈哈").append("\001");
//		System.out.println(sb.toString());
//		sb.deleteCharAt(sb.length()-1);
//		System.out.println(sb.toString());
	}

}
