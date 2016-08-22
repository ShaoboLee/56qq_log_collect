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
 * ������kafka��ȡ������json���ݣ���������������ֶ�"logs","_terminal_","common","_dfp_",�����ڣ��ͻᵱ�������ݴ���
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
	//Monitor monitor;
	String monitorTopic;
	 
	public ParseKafkaDataBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
    public void prepare(Map stormConf, TopologyContext context,
              OutputCollector collector) {
    	this.collector = collector;
    	//p = new KafkaProduce(userConfig,collector);
    	//monitor = new Monitor(userConfig);
    	monitorTopic = Utils.getValue(userConfig, Utils.MONITOR_TOPIC, "monitor");
    }

    public void execute(Tuple input) {
    	
    	JSONObject jb1;
        String json = input.getString(0);
         
         try{
 			jb1 = JSONObject.parseObject(json); 
 			
         }catch(Exception e){
 			collector.emit(input,new Values(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, 
 					"wrong-data-topic"),"",json,"data format is invalid",e.toString()));
 			collector.ack(input);
 			return;
         }
         
         Set<String> sets;//log task id
         
         JSONObject jb2;
         try{
        	 jb2 = jb1.getJSONObject("logs");
        	 sets = jb2.keySet();
         }catch (Exception ex) {//"logs is not exists"
        	 collector.emit(input,new Values(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, 
  					"wrong-data-topic"),"",json,"loss field logs",ex.toString()));
        	 collector.ack(input);
        	 return;
         }
         
         //������һ�����logs֮���key������������Ժ���뵽common����
         Set<String> set1;//keys
         set1 = jb1.keySet();
         HashMap<String,String> map = new HashMap<String, String>();
         for(String k:set1){
        	 if(!k.equals("logs")){
        		 map.put(k, jb1.getString(k));
        	 }
         }
         
         int i = 0;
         
         JSONObject jb3;
         for(String id:sets){
        	 
				JSONArray ja = jb2.getJSONArray(id);
				Object[] obs = ja.toArray();
				
				for(Object ob:obs){
					try{
						jb3 = JSONObject.parseObject(ob.toString());
						String _dfp_ = jb3.getJSONObject("common").getString("_dfp_").toString();
						
						if(!map.isEmpty()){//���ӵ�һ���key��common����
							Iterator iter = map.entrySet().iterator();
							while (iter.hasNext()) {
								Map.Entry entry = (Map.Entry) iter.next();
								((JSONObject)ob).getJSONObject("common").put(entry.getKey().toString(), entry.getValue().toString());
							}
						}
						collector.emit(input,new Values(id,_dfp_,ob.toString(),"",""));
					}catch (Exception ex) {//get _dfp_ fail
						//logger.error("loss field _dfp_, json="+json,ex);
			            if(i==0){//send one time 
			            	//p.produce(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, "wrong-data-topic"),null,json);
			           	    collector.emit(input,new Values(Utils.getValue(userConfig, Utils.DEFAULT_RECEIVE_WRONG_DATA_TOPIC, 
			   					"wrong-data-topic"),"",json,"loss field _dfp_",ex.toString()));
			            	//monitor.produce(monitorTopic, null, monitor.getJson(userConfig.get(Utils.TOPIC).toString(), ex, "field:\"_dfp_\" is loss", json));
			            }
			            i++;
			        }
				}
         }
         collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("topic","_dfp_","json","message","exception"));
                 
    }
}