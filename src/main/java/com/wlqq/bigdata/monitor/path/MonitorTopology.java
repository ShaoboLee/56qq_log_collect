package com.wlqq.bigdata.monitor.path;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.wlqq.bigdata.utils.Utils;

public class MonitorTopology {
	
private static final Log logger = LogFactory.getLog(MonitorTopology.class);
	
	public static void main(String[] args){
		
        //以storm和kafka相关参数的配置文件作为参数		
		if(args.length!=1){
			System.err.println("Usage:./bin/storm jar xxx.jar com.wlqq.bigdata.monitor.MonitorTopology xxxxx.yaml");
			System.exit(0);
		}
		
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/monitor.yaml":args[0], true);
        
        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("monitor-spout", new MonitorSpout(userConfig), Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1)); // Kafka我们创建了一个10分区的Topic，这里并行度设置为10
//        builder.setBolt("monitor-bolt",new MonitorBolt(userConfig) ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_1, 1)).shuffleGrouping("monitor-spout");
       
//        builder.setSpout("monitor-spout", new MonitorSpout(userConfig), Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1)); // Kafka我们创建了一个10分区的Topic，这里并行度设置为10
//        builder.setBolt("find-valid-file-bolt",new FindValidFileBolt(userConfig) ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_1, 1)).shuffleGrouping("monitor-spout");
//        builder.setBolt("compress-load-bolt",new LoadFileBolt(userConfig) ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_2, 1)).shuffleGrouping("find-valid-file-bolt");
    
        builder.setSpout("monitor-spout", new MonitorDirectorySpout(userConfig), Utils.getValue(userConfig, Utils.READER_PARALLELISM, 1)); // Kafka我们创建了一个10分区的Topic，这里并行度设置为10
        builder.setBolt("find-valid-file-bolt",new FindFileByTaskIDBolt(userConfig) ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_1, 1)).shuffleGrouping("monitor-spout");
        builder.setBolt("compress-load-bolt",new CompressAndLoadBolt(userConfig) ,Utils.getValue(userConfig, Utils.LOADER_PARALLELISM_2, 1)).shuffleGrouping("find-valid-file-bolt");
        
        Config conf = new Config();
        conf.put(Config.NIMBUS_HOST, userConfig.get(Utils.NIMBUS_HOST));
        conf.setNumWorkers(Utils.getValue(userConfig, Utils.WORKS_NUMBER, 1));
        conf.putAll(userConfig);
        conf.setDebug(Utils.getValue(userConfig, Utils.STORM_DEBUG, false));
        String name = userConfig.get(Utils.TOPOLOGY_NAME).toString();
        
        if(args.length==0){//local
        	LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("monitor", conf,  
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
