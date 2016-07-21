package com.wlqq.bigdata.kafka;

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
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.wlqq.bigdata.es.EScheme;
import com.wlqq.bigdata.utils.Utils;

public class TestTopology {

	private static final Log logger = LogFactory.getLog(TestTopology.class);

	public static void main(String[] args) throws ClassNotFoundException{
		
//		if(args.length!=1){
//			System.err.println("Usage:./bin/storm jar xxx.jar com.wlqq.bigdata.es.ESTopology xxxxx.yaml");
//			System.exit(0);
//		}
		
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
        
        spoutConf.scheme = new SchemeAsMultiScheme(new EScheme());
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout-kafka-reader", new KafkaSpout(spoutConf),
        		Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1));
        builder.setBolt("count", new TestBolt1(), Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_1, 1))
		.shuffleGrouping("spout-kafka-reader");
      
        Config conf = new Config();
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
