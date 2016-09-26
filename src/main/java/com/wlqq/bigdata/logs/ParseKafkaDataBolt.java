package com.wlqq.bigdata.logs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;
import com.wlqq.bigdata.utils.Utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 解析从kafka读取过来的json数据，如果遇到不存在字段"logs","_terminal_","common","_dfp_",不存在，就会当错误数据处理
 * @author wangchi
 *
 */
public class ParseKafkaDataBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(ParseKafkaDataBolt.class);
    private static final long serialVersionUID = 886149197481637894L;
	
	//ProducerConfig config;
	OutputCollector collector;
	//KafkaProduce p;
	Map<String, Object> userConfig;
	 
	public ParseKafkaDataBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
    public void prepare(Map stormConf, TopologyContext context,
              OutputCollector collector) {
    	this.collector = collector;
    }

	public void execute(Tuple input) {

		JSONObject jb1;
		String json = input.getString(0);

		try {
			jb1 = JSONObject.parseObject(json);

		} catch (Exception e) {
			collector.emit(
					input,
					new Values(Utils.getValue(userConfig,
							Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC,
							"wrong-data-topic"), json,
							"data format is invalid", e.toString()));
			collector.ack(input);
			return;
		}

		Set<String> sets;// log task id

		JSONObject jb2;
		try {
			jb2 = jb1.getJSONObject("logs");
			sets = jb2.keySet();
		} catch (Exception ex) {// "logs is not exists"
			collector.emit(
					input,
					new Values(Utils.getValue(userConfig,
							Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC,
							"wrong-data-topic"), json, "loss field logs", ex
							.toString()));
			collector.ack(input);
			return;
		}

		// 遍历第一层除了logs之外的key，存放起来，稍后加入到common里面
		Set<String> set1;// keys
		set1 = jb1.keySet();
		HashMap<String, String> map = new HashMap<String, String>();
		for (String k : set1) {
			if (!k.equals("logs")) {
				map.put(k, jb1.getString(k));
			}
		}

		for (String id : sets) {

			JSONArray ja = jb2.getJSONArray(id);
			Object[] obs = ja.toArray();

			for (Object ob : obs) {
				if (!map.isEmpty()) {// 添加第一层的key到common里面
					Iterator iter = map.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						((JSONObject) ob).getJSONObject("common").put(
								entry.getKey().toString(),
								entry.getValue().toString());
					}
				}
				collector.emit(input, new Values(id, ob.toString(), "", ""));
			}
		}
		collector.ack(input);
	}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("topic","json","message","exception"));
                 
    }
}
