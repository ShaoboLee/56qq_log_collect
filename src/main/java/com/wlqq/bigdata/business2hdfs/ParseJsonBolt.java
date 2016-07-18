package com.wlqq.bigdata.business2hdfs;

import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.wlqq.bigdata.utils.KafkaProduce;
import com.wlqq.bigdata.utils.Utils;

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
	JSONObject jb;
	OutputCollector collector;
	Map<String, Object> userConfig;
	KafkaProduce p;

	
	public ParseJsonBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		p = new KafkaProduce(userConfig,collector);
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
        String json = input.getString(0);
        
        try{
			jb = JSONObject.parseObject(json); 
		}catch(Exception e){
			logger.error(e);
			//解析失败的数据，发送到相应的topic
			p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json,input);
			//collector.ack(input);
			return;
		}
        
        collector.emit(input,new Values(json));
    	collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("record"));
		
	}

}
