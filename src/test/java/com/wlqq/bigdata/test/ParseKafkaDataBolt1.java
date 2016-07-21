package com.wlqq.bigdata.test;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

//import net.sf.json.JSONArray;
//import net.sf.json.JSONObject;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;
import com.wlqq.bigdata.utils.KafkaProduce;
import com.wlqq.bigdata.utils.Utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerConfig;

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
public class ParseKafkaDataBolt1 extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(ParseKafkaDataBolt1.class);
    private static final long serialVersionUID = 886149197481637894L;
	JSONObject jb1,jb2,jb3;
	ProducerConfig config;
	OutputCollector collector;
	KafkaProduce p;
	Map<String, Object> userConfig;
	 
	public ParseKafkaDataBolt1(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
    public void prepare(Map stormConf, TopologyContext context,
              OutputCollector collector) {
    	this.collector = collector;
    	p = new KafkaProduce(userConfig,collector);
    	
    }

    public void execute(Tuple input) {
    	
         String json = input.getString(0);
         
         try{
 			jb1 = JSONObject.parseObject(json); 
 			
         }catch(Exception e){
 			logger.error("parse json fail, json="+json,e);
 			//发送失败，会调用collector.fail(input)，从而会重新发送
 			p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json,input);
 			//collector.ack(input);
 			return;
         }
         
         Set<String> sets;//log task id
         
         try{
        	 jb2 = jb1.getJSONObject("logs");
        	 sets = jb2.keySet();
         }catch (Exception ex) {//"logs is not exists"
        	 logger.error("loss field logs, json="+json,ex);
        	 p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json,input);
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
						String _dfp_ = jb3.getJSONObject("common").getString("_dfp_").toString();
						if(_terminal_!=null){//add terminal
							((JSONObject)ob).getJSONObject("common").put("_terminal_", _terminal_);
						}
						collector.emit(input,new Values(id,_dfp_,ob.toString()));
					}catch (Exception ex) {//get _dfp_ fail
						logger.error("loss field _dfp_, json="+json,ex);
			            if(i==0){//send one time 
			            	p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json,input);
			            }
			            i++;
			        }
				}
         }
         //collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("topic","_dfp_","json"));
                 
    }
}
