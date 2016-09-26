package com.wlqq.bigdata.business.tokafka;

import java.io.IOException;
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

import com.wlqq.bigdata.utils.Utils;

/**
 * ��ȡkafka����ĸ���topic������֮��д����Ӧ��hive��������棨ָ���ֶηָ�����Ż��߾���json��ʽ��ţ�
 * storm��failԭ��һ��bolt���������fail���ͻ����Kafkaspout�����fail����ʱ���ж�������¼��offset�Ƿ�ǰzookeeper�����offset������maxoffsetbehind��
 * �����ͻᶪ��������¼�����򣬼���һ��ʧ�ܶ��С�ʧ�ܶ��д��������ʧ�ܶ���û�ж����ļ�¼�����Ҽ�����ÿ����¼�´ο���ִ�е�ʱ���������Ե�ʱ���
 * ��������ʧ�ܴ������Ӷ����ӵģ�Ĭ����ȴ�1���ӣ���ÿ����һ��nextTuple�������ǲ鿴ʧ�ܶ��������Ƿ��п���ִ�еļ�¼�����������Լ������
 * �еĻ����ȴ�����Щʧ�ܼ�¼������ʹ�kafka��ȡ�µļ�¼����Ĭ������£���ĳ������������ʧ�ܼ�¼���Ͳ���ȡ����������¼�¼���������ǲ��ϵ�����ʧ�ܼ�¼��
 * ������Ҫ�������¼���������spoutConf.retryDelayMultiplier��spoutConf.retryInitialDelayMs��spoutConf.maxOffsetBehind
 * ���⻹��Ҫע�⣬Namenode��HA���⣬����Config���浥����һ��key�����HA�õ���������Ϣ
 * ���topologyĿǰ�����ڵ����⣺
 * 1����hdfsдʧ�ܺ�û��������
 * 
 * @author wangchi
 *
 */
public class Business2KafkaTopology {
	
	private static final Log logger = LogFactory.getLog(Business2KafkaTopology.class);
	
	public static void main(String[] args) throws ClassNotFoundException{
		
        //��storm��kafka��ز����������ļ���Ϊ����		
//		if(args.length!=1){
//			System.err.println("Usage:./bin/storm jar xxx.jar com.wlqq.bigdata.business.tokafka.Business2KafkaTopology xxxxx.yaml");
//			System.exit(0);
//		}
		
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/storm-kafka.yaml":args[0], true);
		BrokerHosts brokerHosts = new ZkHosts(userConfig.get(Utils.ZKS).toString());
		String topicName = userConfig.get(Utils.TOPIC).toString();
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topicName, userConfig.get(Utils.ZK_ROOT).toString(),
				userConfig.get(Utils.KAFKA_GROUP_ID).toString());
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.ignoreZkOffsets = Utils.getValue(userConfig, Utils.IGNORE_ZK_OFFSETS,false);//�Ƿ��zookeeper��ȡoffset
        
        //fail�ļ�¼����ʱ��������
        spoutConf.retryDelayMultiplier = Utils.getValue(userConfig, Utils.Retry_Delay_Multiplier,spoutConf.retryDelayMultiplier);
        spoutConf.retryInitialDelayMs = Utils.getValue(userConfig, Utils.Retry_Initial_Delay_Ms,spoutConf.retryInitialDelayMs);
        //fail�ļ�¼���Ӷ��ٵ�ʱ����        
        spoutConf.maxOffsetBehind = Utils.getValue(userConfig, Utils.MAX_OFFSET_BEHIND,spoutConf.maxOffsetBehind);
        spoutConf.startOffsetTime = Utils.getValue(userConfig, Utils.START_OFFSET_TIME,kafka.api.OffsetRequest.EarliestTime());
        
        ParseJsonBolt parseJsonBolt = new ParseJsonBolt(userConfig);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1)); // Kafka���Ǵ�����һ��10������Topic�����ﲢ�ж�����Ϊ10
        builder.setBolt("to-kafka",parseJsonBolt ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_1, 1)).shuffleGrouping("kafka-reader");
        
        builder.setBolt("falt-tolerant", new FaultTolerantBolt(), Utils.getValue(userConfig, Utils.TOLERANT_PARALLELISM, 1))
        .shuffleGrouping("to-kafka", Utils.SUCCESS_STREAM)
        .shuffleGrouping("to-kafka", Utils.FAILED_UNEXPECTED_STREAM)
		.shuffleGrouping("to-kafka", Utils.RAWDATA_FORMAT_ERROR_STREAM);
       
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
            cluster.submitTopology("business-kafka", conf,  
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