package com.wlqq.bigdata.logs;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.wlqq.bigdata.utils.Utils;

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

/**
 * 从kafka读取json格式的日志，在storm里面进行日志拆分，解析异常的，写入异常数据对应的topic，解析成功，就把各个业务记录写入到相应的topic里面。
 * 由于storm在处理过程中，是将一个json拆分成了多个子json往后发送，这种情况下，某个子json往kafka写入失败，如果调用fail的话，就会出现其它子json
 * 重复往kafka里面写的问题，所以，子json永远都是调用ack，如果遇到子json发送kafka失败，需要把这条记录输出到某个地方（暂时没有做）。
 * 对于那种在kafka里面不存在的topic，就把这条记录存放到${user.kafka.default.unknown.topic}
 * 
 * @author wangchi
 *
 */
public class LogCollectTopology {
	
	private static final Log logger = LogFactory.getLog(LogCollectTopology.class);
	
	public static void main(String[] args) throws ClassNotFoundException{
		
        //以storm和kafka相关参数的配置文件作为参数		
//		if(args.length!=1){
//			System.err.println("Usage:./bin/storm jar xxx.jar com.wlqq.bigdata.log.LogCollectTopology xxxxx.yaml");
//			System.exit(0);
//		}
		
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/storm-kafka.yaml":args[0], true);
		BrokerHosts brokerHosts = new ZkHosts(userConfig.get(Utils.ZKS).toString());
		String topicName = userConfig.get(Utils.TOPIC).toString();
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topicName, userConfig.get(Utils.ZK_ROOT).toString(),
				userConfig.get(Utils.KAFKA_GROUP_ID).toString());
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.ignoreZkOffsets=Utils.getValue(userConfig, Utils.IGNORE_ZK_OFFSETS,false);//是否从zookeeper读取offset
        
        //fail的记录重试时间间隔配置
        spoutConf.retryDelayMultiplier = Utils.getValue(userConfig, Utils.Retry_Delay_Multiplier,spoutConf.retryDelayMultiplier);
        spoutConf.retryInitialDelayMs = Utils.getValue(userConfig, Utils.Retry_Initial_Delay_Ms,spoutConf.retryInitialDelayMs);
        //fail的记录掉队多少的时候丢弃 
        spoutConf.maxOffsetBehind = Utils.getValue(userConfig, Utils.MAX_OFFSET_BEHIND,spoutConf.maxOffsetBehind);
        spoutConf.startOffsetTime = Utils.getValue(userConfig, Utils.START_OFFSET_TIME,kafka.api.OffsetRequest.EarliestTime());
        
        ParseKafkaDataBolt parseJsonBolt = new ParseKafkaDataBolt(userConfig);
        StormToKafkaBolt stormToKafkaBolt = new StormToKafkaBolt(userConfig);
        DealInvalidDataBolt dealInvalidDataBolt = new DealInvalidDataBolt(userConfig);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1)); // Kafka我们创建了一个10分区的Topic，这里并行度设置为10
        builder.setBolt("parse-json",parseJsonBolt ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_1, 1)).shuffleGrouping("kafka-reader");
        builder.setBolt("deal-invalid-data",dealInvalidDataBolt ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_2, 1)).shuffleGrouping("parse-json");
        builder.setBolt("storm-to-kafka", stormToKafkaBolt,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_3, 1)).fieldsGrouping("deal-invalid-data",Utils.KAFKA_WRITE_DATA_STREAM, new Fields("_dfp_"));
        
        builder.setBolt("falt-tolerant", new FaultTolerantBolt(), Utils.getValue(userConfig, Utils.TOLERANT_PARALLELISM, 1))
        .shuffleGrouping("storm-to-kafka", Utils.SUCCESS_STREAM)
        .shuffleGrouping("storm-to-kafka", Utils.KAFKA_WRITE_FAIL_STREAM)
		.shuffleGrouping("deal-invalid-data", Utils.RAWDATA_FORMAT_ERROR_STREAM)
		.shuffleGrouping("deal-invalid-data", Utils.UNKNOWN_TOPIC_STREAM);
        
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
