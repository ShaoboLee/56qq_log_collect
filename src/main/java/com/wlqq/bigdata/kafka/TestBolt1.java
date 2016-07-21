package com.wlqq.bigdata.kafka;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift7.TException;


import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.NimbusClient;

public class TestBolt1 extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(TestBolt1.class);
	
	long i = 0;
	long j = 0;
	OutputCollector collector;
	private Thread executor = null;
	Client client;
	long count = 0;
	long total = 0l;
	long begin;
	boolean flag = true;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
/*		client = NimbusClient.getConfiguredClient(stormConf).getClient();
		 executor = new Thread(new Runnable() {
		    	long current = System.currentTimeMillis();
				public void run() {
					logger.info("thread start...");
					while(true){
						try {
							List<TopologySummary> ts = client.getClusterInfo().get_topologies();
							for(TopologySummary t:ts){
								System.out.println("name="+t.get_name());
								System.out.println("id="+t.get_id());
								System.out.println("get_num_executors="+t.get_num_executors());
								System.out.println("get_status="+t.get_status());
								System.out.println("get_uptime_secs="+t.get_uptime_secs());
								System.out.println("toString="+t.toString());
								System.out.println("***********************************************");
							}
							Thread.sleep(5000);
						} catch (AuthorizationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
		 });
		 executor.start();*/
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
//		i++;
//		j++;
		
		if(flag){
			begin = System.currentTimeMillis();
			flag = false;
		}
		
		long start = System.currentTimeMillis();
		
//		if(System.currentTimeMillis()-current>=10*1000){
//			long cost = (System.currentTimeMillis()-current)/1000;
//			long num = j/cost;
//			logger.info("花费了 "+cost+"s,消费了"+j+"条,平均速度是每秒"+num+"条");
//			current = System.currentTimeMillis();
//			j = 0;
//		}
		
		count++;
		long end = System.currentTimeMillis();
		total +=(end-start);
//		
//		if(count%10000==0){
//			System.out.println("spout parse json "+count+"条,cost time:"+total/1000+"s,"+"-----"+(System.currentTimeMillis()-begin)/1000+"s");
//		}
		
		
		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
