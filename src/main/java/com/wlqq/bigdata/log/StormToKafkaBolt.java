package com.wlqq.bigdata.log;

import java.util.HashMap;
import java.util.Map;

import kafka.utils.ZkUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wlqq.bigdata.common.KafkaProduce;
import com.wlqq.bigdata.common.Utils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerConfig;

import scala.collection.Seq;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 把storm里面的数据发送到kafka，以设备id作为key（可以保证相同的key发送到相同的partition），json作为value
 * @author wangchi
 *
 */
public class StormToKafkaBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(StormToKafkaBolt.class);
    private static final long serialVersionUID = 886149197481637891L;
	ProducerConfig config;
	Map<String, Object> userConfig;
	ZkClient zc;//find topics
	KafkaProduce p;
	OutputCollector collector;
	
	public StormToKafkaBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
	 
    public void prepare(Map stormConf, TopologyContext context,
              OutputCollector collector) {
    	
    	p =  new KafkaProduce(userConfig);
	    zc = new ZkClient(userConfig.get(Utils.ZKS).toString());
	    this.collector = collector;
    }

    /**
     * 判断topic是否存在，有就把json发送到这个topic，没有就发送到DEFAULT_UNKNOWN_TOPIC（同时把这个未知的topic也加入到了json里面）
     */
    public void execute(Tuple input) {
    	
    	String topic = input.getStringByField("topic");
    	String _dfp_ = input.getStringByField("_dfp_");
    	String json = input.getStringByField("json");
    	
    	//从zookeeper里面获取kafka目前创建的topic列表
    	Seq<String> topics = ZkUtils.getAllTopics(zc);
    	
    	if(!topics.contains(topic)){
    		JSONObject jb = new JSONObject();
    		jb.put(topic, JSONObject.parse(json));
    		json = jb.toJSONString();
    		topic = Utils.getValue(userConfig, Utils.DEFAULT_UNKNOWN_TOPIC, "unknown-topic");
    	}
    	
    	p.produce(topic,_dfp_,json);
    	//永远是ack，因为一个json解析成多个子json后，分别发往各自的topic，
    	//只要其中一个是fail就会整个重发，导致数据重复发送，对于fail的情况，单独保存
    	collector.ack(input);
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
                 
    }
    

    public static void main(String[] args){
    	
		String json = "{\"_terminal_\":1,\"logs\":{" +
				"\"1001\":["+"{\"common\":{\"_uid_\":1111,\"b\":100},\"data\":{\"count\":5}}],"+
				"\"1003\":["+"{\"common\":{\"_uid_\":3333,\"d\":300},\"data\":{\"count\":55}}],"+
				"\"1002\":["+"{\"common\":{\"_uid_\":2222,\"c\":200},\"data\":{\"count\":50}}]}}";
		
		String a="1234";
		System.out.println(a.substring(0,a.length()-1));
		
//		HashMap<String,String> map = new HashMap<String, String>();
//		map.put("a", "1");
//		HashMap<String,String> map1 =(HashMap<String,String>) map.clone();
//		map1.put("a", "2");
//		System.out.println(map);
//		System.out.println(map1);
//		String json = "{\"logs\":{\"1001\":[{\"common\":{\"a\":1,\"_uid_\":1111},{\"b\":100}}]," +
//				"\"1002\":[{\"common\":{\"c\":1,\"_uid_\":2222},{\"d\":55}]}}";
/*		StormToKafkaBolt sp = new StormToKafkaBolt(new HashMap<String, Object>());
		sp.prepare(null,null,null);
		int i=0;
		while(i<100){
			sp.produce("test-1005","111",json);
			i++;
		}*/
/*		
		String[] options = new String[]{  
			    "--list",  
			    "--zookeeper",  
			    "v32:2181,v29:2181,v30:2181"  
			}; 
		//TopicCommand.main(options);
		ZkClient zc = new ZkClient("v32:2181,v29:2181,v30:2181");
		System.out.println(ZkUtils.getAllTopics(zc));
		System.out.println(kafka.api.OffsetRequest.LatestTime());*/
		
//		JSONObject jb = new JSONObject();
//		jb.put("lg", JSONObject.parse(json));
//		//json = jb.;
//		System.out.println(jb.getString("ddd").toString());
//		String j = jb.toJSONString();
//		System.out.println(j);
		String _terminal_ = JSONObject.parseObject(json).getString("_terminal_");
		JSONObject jb3 = JSONObject.parseObject(json).getJSONObject("logs");
		JSONArray ja = jb3.getJSONArray("1001");
		Object[] obs = ja.toArray();
		for(Object ob:obs){
				if(_terminal_!=null){//add terminal
					((JSONObject)ob).getJSONObject("common").put("_terminal_", _terminal_);
				}
				System.out.println(ob.toString());
		}
//	    String _uid_ = jb3.getJSONArray("1001").getJSONObject(0).getJSONObject("common").getString("_uid_");
//	    System.out.println("uid="+_uid_);
//		JSONObject jb1 = JSONObject.parseObject(j);
//		System.out.println(jb1);
//		JSONObject jb2 = jb1.getJSONObject("lg");
//		System.out.println(jb2.get("logs"));
    }

}
