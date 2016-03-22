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
 * 读取kafka里面的各个topic，解析之后，写到对应的hive表分区里面（指定字段分隔符存放或者就以json格式存放）
 * storm的fail原理，一旦bolt里面出现了fail，就会调用Kafkaspout里面的fail，这时会判断这条记录的offset是否当前zookeeper保存的offset超过了maxoffsetbehind，
 * 超过就会丢弃这条记录，否则，加入一个失败队列。失败队列存放了以往失败而还没有丢弃的记录，并且记载了每条记录下次可以执行的时间间隔（重试的时间间
 * 隔是随着失败次数增加而增加的，默认最长等待1分钟），每调用一次nextTuple，首先是查看失败队列里面是否有可以执行的记录（到达了重试间隔），
 * 有的话就先处理这些失败记录，否则就从kafka读取新的记录处理，默认情况下，当某个分区出现了失败记录，就不会取这个分区的新记录来处理，而是不断的重试失败记录，
 * 所以需要配置如下几个参数：spoutConf.retryDelayMultiplier，spoutConf.retryInitialDelayMs，spoutConf.maxOffsetBehind
 * 另外还需要注意，Namenode的HA问题，是在Config里面单独用一个key来存放HA用到的配置信息
 * 这个topology目前还存在的问题：
 * 1）往hdfs写失败后，没有做处理
 * 2）没有考虑文件压缩
 * 3）没有考虑topology在kill后的收尾处理
 * 4）load到hive失败时，没有处理，只能通过手动把数据load到hive
 * 
 * @author wangchi
 *
 */
public class LogLandTopology {
	
	private static final Log logger = LogFactory.getLog(LogLandTopology.class);
	
	public static void main(String[] args){
		
        //以storm和kafka相关参数的配置文件作为参数		
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
        spoutConf.ignoreZkOffsets = Utils.getValue(userConfig, Utils.IGNORE_ZK_OFFSETS,false);//是否从zookeeper读取offset
        
        //fail的记录重试时间间隔配置
        spoutConf.retryDelayMultiplier = Utils.getValue(userConfig, Utils.Retry_Delay_Multiplier,spoutConf.retryDelayMultiplier);
        spoutConf.retryInitialDelayMs = Utils.getValue(userConfig, Utils.Retry_Initial_Delay_Ms,spoutConf.retryInitialDelayMs);
        //fail的记录掉队多少的时候丢弃        
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
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1)); // Kafka我们创建了一个10分区的Topic，这里并行度设置为10
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