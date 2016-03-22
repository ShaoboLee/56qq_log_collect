package com.wlqq.bigdata.monitor;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.wlqq.bigdata.common.Utils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MonitorSpout extends BaseRichSpout{
	
	private static final Log logger = LogFactory.getLog(MonitorSpout.class);
	
	private Map<String, Object> userConfig;
	private SpoutOutputCollector collector;
	
	public MonitorSpout(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		for (Map.Entry<String, Object> entry : userConfig.entrySet()){
			if(entry.getKey().startsWith("clientlog_")){
				Object paras = entry.getValue();
				if(paras==null){
					logger.error("para:"+entry.getKey()+" is lack of values");
					continue;
				}else{
					collector.emit(new Values(paras.toString()));
				}
			}
		}
		
		int sleepSec = Utils.getValue(userConfig, Utils.SPOUT_SEND_INTERVAL_SEC, 60);
		try {
			logger.info("sleep "+sleepSec+"s");
			Thread.currentThread().sleep(sleepSec*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("paras"));
		
	}

}
