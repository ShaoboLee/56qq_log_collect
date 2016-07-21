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
 * ��kafka��ȡjson��ʽ����־����storm���������־��֣������쳣�ģ�д���쳣���ݶ�Ӧ��topic�������ɹ����ͰѸ���ҵ���¼д�뵽��Ӧ��topic���档
 * ����storm�ڴ�������У��ǽ�һ��json��ֳ��˶����json�����ͣ���������£�ĳ����json��kafkaд��ʧ�ܣ��������fail�Ļ����ͻ����������json
 * �ظ���kafka����д�����⣬���ԣ���json��Զ���ǵ���ack�����������json����kafkaʧ�ܣ���Ҫ��������¼�����ĳ���ط�����ʱû��������
 * ����������kafka���治���ڵ�topic���Ͱ�������¼��ŵ�${user.kafka.default.unknown.topic}
 * 
 * @author wangchi
 *
 */
public class LogCollectTopology {
	
	private static final Log logger = LogFactory.getLog(LogCollectTopology.class);
	
	public static void main(String[] args) throws ClassNotFoundException{
		
        //��storm��kafka��ز����������ļ���Ϊ����		
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
        spoutConf.ignoreZkOffsets=Utils.getValue(userConfig, Utils.IGNORE_ZK_OFFSETS,false);//�Ƿ��zookeeper��ȡoffset
        
        //fail�ļ�¼����ʱ��������
        spoutConf.retryDelayMultiplier = Utils.getValue(userConfig, Utils.Retry_Delay_Multiplier,spoutConf.retryDelayMultiplier);
        spoutConf.retryInitialDelayMs = Utils.getValue(userConfig, Utils.Retry_Initial_Delay_Ms,spoutConf.retryInitialDelayMs);
        //fail�ļ�¼���Ӷ��ٵ�ʱ���� 
        spoutConf.maxOffsetBehind = Utils.getValue(userConfig, Utils.MAX_OFFSET_BEHIND,spoutConf.maxOffsetBehind);
        spoutConf.startOffsetTime = Utils.getValue(userConfig, Utils.START_OFFSET_TIME,kafka.api.OffsetRequest.EarliestTime());
        
        ParseKafkaDataBolt parseJsonBolt = new ParseKafkaDataBolt(userConfig);
        StormToKafkaBolt stormToKafkaBolt = new StormToKafkaBolt(userConfig);
        DealInvalidDataBolt dealInvalidDataBolt = new DealInvalidDataBolt(userConfig);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1)); // Kafka���Ǵ�����һ��10������Topic�����ﲢ�ж�����Ϊ10
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
