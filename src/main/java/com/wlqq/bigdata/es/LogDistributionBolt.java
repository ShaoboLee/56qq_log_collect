package com.wlqq.bigdata.es;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.wlqq.bigdata.common.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogDistributionBolt extends BaseRichBolt {

	private static final Log logger = LogFactory.getLog(LogDistributionBolt.class);
	OutputCollector collector;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		this.collector = collector;
	}

	public void execute(Tuple input) {
		
		String json = input.getString(0);
		String key = null;
		try{
			
			JSONObject jo = JSONObject.parseObject(json);
			String host = jo.getJSONObject("file_location").getString("host");
			String service = jo.getJSONObject("file_location").getString("service");
			String docker = jo.getJSONObject("file_location").getString("docker");
			key = host+"-"+service+"-"+docker;
		}catch(Exception e) {
			logger.error(e);
		}
		
		if(key==null){
			Result res = new Result(Result.STATUS.FAILED_RAWDATA_FORMAT_ERROR, "JSON is invalid or json don't contain host¡¢service¡¢docker", json);
			res.setTuple(input);
			emitResult(Utils.RAWDATA_FORMAT_ERROR_STREAM, res);
		}else{
			collector.emit(Utils.DISTRIBUTION_STREAM, input,new Values(json,key));
		}
		collector.ack(input);
	}
	
	private void emitResult(String stream, Result res) {
		List<Object> outputTuple = new ArrayList<Object>(1);
		String rawDoc = res.getTuple().getString(0);
		Result r = res.asNoTupleResult();
		r.setRawJSONDocument(rawDoc);
		outputTuple.add(r);
		collector.emit(stream, outputTuple);
	};

	public void declareOutputFields(OutputFieldsDeclarer declare) {

		declare.declareStream(Utils.DISTRIBUTION_STREAM, new Fields("json","key"));
		declare.declareStream(Utils.RAWDATA_FORMAT_ERROR_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
	}

}
