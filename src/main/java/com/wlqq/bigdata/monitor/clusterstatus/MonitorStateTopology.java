package com.wlqq.bigdata.monitor.clusterstatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift7.TException;

import com.wlqq.bigdata.utils.Utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.NimbusClient;

public class MonitorStateTopology {
	
	private static final Log logger = LogFactory.getLog(MonitorStateTopology.class);
	private static class MonitorStateSpout extends BaseRichSpout{
		
		private Thread executor = null;
		Client client;
		OutputCollector collector;
		Map<String, Object> userConfig;
		Map<String, Long> map;
		
		public MonitorStateSpout(Map<String, Object> userConfig){
			this.userConfig = userConfig;
		}

		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {

			map = new HashMap<String,Long>();
			this.collector = this.collector;
			client = NimbusClient.getConfiguredClient(conf).getClient();
			executor = new Thread(new Runnable() {
		    	long current = System.currentTimeMillis();
				public void run() {
					logger.info("thread start...");
					List<TopologySummary> ts;
					while (true) {
						try {
							ts = client.getClusterInfo().get_topologies();
							for(TopologySummary t:ts){
								if(t.get_status().equalsIgnoreCase("INACTIVE") && !map.containsKey(t.get_name())){
									logger.info("topology:"+t.get_name()+",status:deactivate,this topology will change status after "
								                +Utils.getValue(userConfig, Utils.TOPOLOGY_SUSPEND_TIME_SEC, 60)+"s");
									map.put(t.get_name(), System.currentTimeMillis());
								}
							}
						} catch (AuthorizationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
						
						if(!map.isEmpty()){
							Map<String,Long> map1;
							map1 = (Map<String, Long>) ((HashMap)map).clone();
							logger.info("map:"+map);
							for (Entry<String, Long> entry : map1.entrySet()) {
								Exception ex = null;
								if(System.currentTimeMillis()-entry.getValue()>=Utils.getValue(userConfig, Utils.TOPOLOGY_SUSPEND_TIME_SEC, 60)*1000){
									try {
										client.activate(entry.getKey());
									} catch (NotAliveException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
										ex = e;
									} catch (AuthorizationException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
										ex = e;
									} catch (TException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
										ex = e;
									}
									if(ex==null){
										logger.info("activate success...");
										map.remove(entry.getKey());
										
									}else{
										logger.error("activate fail...", ex);
									}
								}
							}
						}
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
				}
					
			});
			executor.start();
		}

		public void nextTuple() {
			// TODO Auto-generated method stub
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}
		
	}
	public static void main(String[] args){
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/storm-kafka.yaml":args[0], true);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout1", new MonitorStateSpout(userConfig),1);
		 Config conf = new Config();
		 conf.put(Config.NIMBUS_HOST, userConfig.get(Utils.NIMBUS_HOST));
	     conf.setNumWorkers(Utils.getValue(userConfig, Utils.WORKS_NUMBER, 1));
	     conf.putAll(userConfig);
	     conf.setDebug(Utils.getValue(userConfig, Utils.STORM_DEBUG, false));
	     String name = userConfig.get(Utils.TOPOLOGY_NAME).toString();
	       
	     if(args.length==0){//local
	    	 LocalCluster cluster = new LocalCluster();  
	    	 cluster.submitTopology("kafka-storm-log", conf,  builder.createTopology()); 
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
