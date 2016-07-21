package com.wlqq.bigdata.business.tokafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.wlqq.bigdata.monitor.Monitor;
import com.wlqq.bigdata.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class FaultTolerantBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(FaultTolerantBolt.class);
	
	public void execute(Tuple tuple) {

	}
	
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		
    }
		
	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}
}
