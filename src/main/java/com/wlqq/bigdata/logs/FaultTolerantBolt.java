package com.wlqq.bigdata.logs;

import java.util.Map;

import org.apache.log4j.Logger;

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
//		String streamID = tuple.getSourceStreamId();
//		if (!Utils.SUCCESS_STREAM.equals(streamID)) {
//			StringBuffer sb = new StringBuffer();
//			sb.append("topic:"+tuple.getStringByField("topic")).append(",")
//			.append("message:").append(tuple.getStringByField("message")).append(",")
//			.append("exception:").append(tuple.getStringByField("exception")).append(",")
//			.append("json=").append(tuple.getStringByField("json"));
//			
//			//logger.info("###Failed result,StreamID:"+streamID+",result:" + sb.toString());
//		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}
}
