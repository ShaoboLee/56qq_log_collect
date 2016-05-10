package com.wlqq.bigdata.es;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.wlqq.bigdata.utils.Utils;

public class ESTopology {

	private static final Log logger = LogFactory.getLog(ESTopology.class);

	public static void main(String[] args) throws ClassNotFoundException{
		
		if(args.length!=1){
			System.err.println("Usage:./bin/storm jar xxx.jar com.wlqq.bigdata.es.ESTopology xxxxx.yaml");
			System.exit(0);
		}
		
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/es.yaml":args[0], true);
		BrokerHosts brokerHosts = new ZkHosts(userConfig.get(Utils.ZKS).toString());
		String topicName = userConfig.get(Utils.TOPIC).toString();
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topicName, userConfig.get(Utils.ZK_ROOT).toString(),
				userConfig.get(Utils.KAFKA_GROUP_ID).toString());
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.ignoreZkOffsets = Utils.getValue(userConfig, Utils.IGNORE_ZK_OFFSETS,false);//是否从zookeeper读取offset
        
        //fail的记录重试时间间隔配置
        spoutConf.retryDelayMultiplier = Utils.getValue(userConfig, Utils.Retry_Delay_Multiplier,spoutConf.retryDelayMultiplier);
        spoutConf.retryInitialDelayMs = Utils.getValue(userConfig, Utils.Retry_Initial_Delay_Ms,spoutConf.retryInitialDelayMs);
        //fail的记录掉队多少的时候丢弃        
        spoutConf.maxOffsetBehind = Utils.getValue(userConfig, Utils.MAX_OFFSET_BEHIND,spoutConf.maxOffsetBehind);
        spoutConf.startOffsetTime = Utils.getValue(userConfig, Utils.START_OFFSET_TIME,kafka.api.OffsetRequest.EarliestTime());
        //spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        
        spoutConf.scheme = new SchemeAsMultiScheme(new EScheme());
        
        BatchESLoadBolt loadBolt = new BatchESLoadBolt(userConfig);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        LogDistributionBolt distributionBolt = new LogDistributionBolt();
        
        builder.setSpout("spout-kafka-reader", new KafkaSpout(spoutConf),
        		Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1));
        
        builder.setBolt("bolt-distribution", distributionBolt, Utils.getValue(userConfig, Utils.DISTRIBUTION_PARALLELISM, 1))
        .fieldsGrouping("spout-kafka-reader", new Fields("key"));
		//.shuffleGrouping("spout-kafka-reader");
        
        builder.setBolt("bolt-es-loader", loadBolt, Utils.getValue(userConfig, Utils.LOADER_PARALLELISM, 1))
		.fieldsGrouping("bolt-distribution",Utils.DISTRIBUTION_STREAM, new Fields("key"));

        builder.setBolt("falt-tolerant", new FaultTolerantBolt(userConfig), Utils.getValue(userConfig, Utils.TOLERANT_PARALLELISM, 1))
        .shuffleGrouping("bolt-es-loader", Utils.SUCCESS_STREAM)
		.shuffleGrouping("bolt-es-loader", Utils.RAWDATA_FORMAT_ERROR_STREAM)
		.shuffleGrouping("bolt-es-loader", Utils.ES_RESULT_ERROR_STREAM)
		.shuffleGrouping("bolt-es-loader", Utils.ES_EXECUTE_EXCEPTION_STREAM)
		.shuffleGrouping("bolt-es-loader", Utils.FAILED_RECOVERABLE_STREAM)
		.shuffleGrouping("bolt-es-loader", Utils.FAILED_UNEXPECTED_STREAM)
		.shuffleGrouping("bolt-es-loader", Utils.QUEUE_TIMEOUT_STREAM)
		.shuffleGrouping("bolt-es-loader", Utils.FAILED_REJECT_STREAM)
		.shuffleGrouping("bolt-es-loader", Utils.FAILED_MAPPING_STREAM)
		.shuffleGrouping("bolt-distribution", Utils.RAWDATA_FORMAT_ERROR_STREAM);
        
        
//		builder.setBolt("bolt-es-loader", loadBolt, Utils.getValue(userConfig, Utils.LOADER_PARALLELISM, 1))
//				.shuffleGrouping("spout-kafka-reader");
//		builder.setBolt("falt-tolerant", new FaultTolerantBolt(), Utils.getValue(userConfig, Utils.TOLERANT_PARALLELISM, 1))
//		        .shuffleGrouping("bolt-es-loader", Utils.SUCCESS_STREAM)
//				.shuffleGrouping("bolt-es-loader", Utils.RAWDATA_FORMAT_ERROR_STREAM)
//				.shuffleGrouping("bolt-es-loader", Utils.ES_RESULT_ERROR_STREAM)
//				.shuffleGrouping("bolt-es-loader", Utils.ES_EXECUTE_EXCEPTION_STREAM)
//				.shuffleGrouping("bolt-es-loader", Utils.FAILED_RECOVERABLE_STREAM)
//				.shuffleGrouping("bolt-es-loader", Utils.FAILED_UNEXPECTED_STREAM)
//				.shuffleGrouping("bolt-es-loader", Utils.QUEUE_TIMEOUT_STREAM)
//				.shuffleGrouping("bolt-es-loader", Utils.FAILED_REJECT_STREAM)
//				.shuffleGrouping("bolt-es-loader", Utils.FAILED_MAPPING_STREAM);
		
//		Config conf = new Config();
//
//		conf.registerMetricsConsumer(
//				Class.forName(Utils.getString(userConfig, METRICS_CONSUMER, LoggingMetricsConsumer.class.getName())),
//				Utils.getInteger(userConfig, METRICS_PARALLELISM, 1));
//
//		conf.put(Config.NIMBUS_HOST, userConfig.get(NIMBUS_HOST));
//		conf.setNumWorkers(Utils.getInteger(userConfig, WORKS_NUMBER, 1));
//		conf.putAll(userConfig);
//		String name = userConfig.get(TOPOLOGY_NAME).toString();
//
//		StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        
        Config conf = new Config();
        
		conf.registerMetricsConsumer(
		Class.forName(Utils.getValue(userConfig, Utils.METRICS_CONSUMER, LoggingMetricsConsumer.class.getName())),
		Utils.getValue(userConfig, Utils.METRICS_PARALLELISM, 1));
        conf.put(Config.NIMBUS_HOST, userConfig.get(Utils.NIMBUS_HOST));
        conf.setNumWorkers(Utils.getValue(userConfig, Utils.WORKS_NUMBER, 1));
        conf.putAll(userConfig);
        conf.setDebug(Utils.getValue(userConfig, Utils.STORM_DEBUG, false));
        String name = userConfig.get(Utils.TOPOLOGY_NAME).toString();
        
        if(args.length==0){//local
        	LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("kafka-storm-log", conf,  
            builder.createTopology()); 
        }else{
            try {
    			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
    		} catch (AlreadyAliveException e) {
    			logger.error(e);
    		} catch (InvalidTopologyException e) {
    			logger.error(e);
    		} catch (AuthorizationException e) {
    			logger.error(e);
    		}
        }
        
   }
}
