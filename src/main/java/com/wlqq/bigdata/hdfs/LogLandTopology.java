package com.wlqq.bigdata.hdfs;

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
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.wlqq.bigdata.common.Utils;
import com.wlqq.bigdata.storm.hdfs.bolt.HdfsBolt;
import com.wlqq.bigdata.storm.hdfs.bolt.format.DefaultFileNameFormat;
import com.wlqq.bigdata.storm.hdfs.bolt.format.DelimitedRecordFormat;
import com.wlqq.bigdata.storm.hdfs.bolt.format.FileNameFormat;
import com.wlqq.bigdata.storm.hdfs.bolt.format.RecordFormat;
import com.wlqq.bigdata.storm.hdfs.bolt.rotation.FileRotationPolicy;
import com.wlqq.bigdata.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import com.wlqq.bigdata.storm.hdfs.bolt.rotation.PartitionByHourPolicy;
import com.wlqq.bigdata.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import com.wlqq.bigdata.storm.hdfs.bolt.sync.CountSyncPolicy;
import com.wlqq.bigdata.storm.hdfs.bolt.sync.SyncPolicy;
import com.wlqq.bigdata.storm.hdfs.common.rotation.LoadDataToHiveAction;
import com.wlqq.bigdata.storm.hdfs.common.rotation.RotationAction;

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
 * 2��û�п����ļ�ѹ��
 * 3��û�п���topology��kill�����β����
 * 4��load��hiveʧ��ʱ��û�д���ֻ��ͨ���ֶ�������load��hive
 * 
 * @author wangchi
 *
 */
public class LogLandTopology {
	
	private static final Log logger = LogFactory.getLog(LogLandTopology.class);
	
	public static void main(String[] args){
		
        //��storm��kafka��ز����������ļ���Ϊ����		
		if(args.length!=1){
			System.err.println("Usage:./bin/storm jar xxx.jar com.wlqq.bigdata.hdfs.LogLandTopology xxxxx.yaml");
			System.exit(0);
		}
		
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/storm-hdfs-1001.yaml":args[0], true);
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
        
        int intervalHour = Utils.getValue(userConfig, Utils.HIVE_PARTITION_HOUR_INTERVAL, 1);
        
        ParseJsonBolt parseJsonBolt = new ParseJsonBolt(userConfig);
        
        RecordFormat format = new DelimitedRecordFormat();

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(Utils.getValue(userConfig, Utils.HDFS_BATCH_SIZE, 1000));

//      FileRotationPolicy rotationPolicy = new PartitionByHourPolicy(Utils.getValue(userConfig, Utils.HIVE_FILE_SIZE, 10f), 
//		com.wlqq.bigdata.storm.hdfs.bolt.rotation.PartitionByHourPolicy.Units.MB,intervalHour);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(Utils.getValue(userConfig, Utils.HIVE_FILE_SIZE, 10f), 
		com.wlqq.bigdata.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units.MB);
        String hdfsurl = Utils.getValue(userConfig, Utils.HDFS_URL,"hdfs://localhost:9000");
        
        //RotationAction action = new LoadDataToHiveAction(intervalHour,userConfig);
        
        String hdfsWritePath = Utils.getValue(userConfig, Utils.HDFS_WRITE_PATH,"/storm");
        
        hdfsWritePath = hdfsWritePath.endsWith("/")?hdfsWritePath+topicName:hdfsWritePath+"/"+topicName;
        
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
        .withPath(hdfsWritePath).withPrefix(topicName+"-").withExtension(".txt");//.withPrefix(topicName+"-");

        HdfsBolt hdfsBolt = new HdfsBolt()
        .withFsUrl(hdfsurl)
        .withInterval(intervalHour)
        .withFileNameFormat(fileNameFormat)
        .withRecordFormat(format)
        .withRotationPolicy(rotationPolicy)
        .withSyncPolicy(syncPolicy)
        //.addRotationAction(action)
        .withConfigKey("hdfs-key");
                            
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1)); // Kafka���Ǵ�����һ��10������Topic�����ﲢ�ж�����Ϊ10
        builder.setBolt("parse-json",parseJsonBolt ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_1, 1)).shuffleGrouping("kafka-reader");
        builder.setBolt("storm-to-hdfs",hdfsBolt ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_2, 1)).shuffleGrouping("parse-json");
       
        Config conf = new Config();
        conf.put(Config.NIMBUS_HOST, userConfig.get(Utils.NIMBUS_HOST));
        conf.setNumWorkers(Utils.getValue(userConfig, Utils.WORKS_NUMBER, 1));
        conf.putAll(userConfig);
        conf.setDebug(Utils.getValue(userConfig, Utils.STORM_DEBUG, false));
        String name = userConfig.get(Utils.TOPOLOGY_NAME).toString();
        Config conf1 =(Config) conf.clone();
        //namenode ha config
        conf1.put("hdfs-key", conf);//namenode HA conf
        
        if(args.length==0){//local
        	LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("kafka-storm-log", conf1,  
            builder.createTopology()); 
        }else{
            try {
    			StormSubmitter.submitTopologyWithProgressBar(name, conf1, builder.createTopology());
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