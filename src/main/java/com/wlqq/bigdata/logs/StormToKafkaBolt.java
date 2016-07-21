package com.wlqq.bigdata.logs;

import java.util.Map;

import com.wlqq.bigdata.utils.KafkaProduce;
import com.wlqq.bigdata.utils.Utils;
//import com.wlqq.bigdata.monitor.Monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift7.TException;


import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.NimbusClient;

/**
 * 把storm里面的数据发送到kafka，以设备id作为key（可以保证相同的key发送到相同的partition），json作为value
 * @author wangchi
 *
 */
public class StormToKafkaBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(StormToKafkaBolt.class);
    private static final long serialVersionUID = 886149197481637891L;
	//ProducerConfig config; 
	Map<String, Object> userConfig;
	//ZkClient zc;//find topics
	KafkaProduce p;
	OutputCollector collector;
//	HashMap<String,String> topics;
//	long begin;
//	long updateTopicsInfoIntervalMs;
	//Monitor monitor;
	String monitorTopic;
	private Thread executor = null;
	Client client;
	private boolean closeFlag = false;
	private boolean closeable = false;
	
	public StormToKafkaBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
	 
	public void activate(){
		boolean success = false;
		Exception ex = null;
		while(!success){
			try {
				client.activate(userConfig.get(Utils.TOPOLOGY_NAME).toString());
				
			} catch (NotAliveException e) {
				// TODO Auto-generated catch block
				ex = e;
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				ex = e;
			} catch (TException e) {
				// TODO Auto-generated catch block
				ex = e;
			} 
			if(ex==null){
				success = true;
				logger.info("activate success...");
			}else{
				logger.error("activate fail...", ex);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/**
	 * 暂停这个topology的执行，spout不在调用nextTuple
	 */
	public void suspend(){
		Exception ex = null;
		try {
			client.deactivate(userConfig.get(Utils.TOPOLOGY_NAME).toString());
			logger.info("suspend "+Utils.getValue(userConfig, Utils.TOPOLOGY_SUSPEND_TIME_SEC, 60)+"s...");
			Thread.sleep(Utils.getValue(userConfig, Utils.TOPOLOGY_SUSPEND_TIME_SEC, 60)*1000);
			p.setContinuityFailCount(0);//重新置0
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			ex = e;
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			ex = e;
		} catch (TException e) {
			// TODO Auto-generated catch block
			ex = e;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(ex==null){//suspend success
			logger.info("deactivate success");
			activate();
		}else{
			logger.error("deactivate fail...",ex);
		}
	}
	
	
    public void prepare(Map stormConf, TopologyContext context,
              OutputCollector collector) {
    	
    	client = NimbusClient.getConfiguredClient(stormConf).getClient();
    	p =  new KafkaProduce(userConfig,collector);
    	//monitor = new Monitor(userConfig);
    	monitorTopic = Utils.getValue(userConfig, Utils.MONITOR_TOPIC, "monitor");
	    this.collector = collector;
	    
	    executor = new Thread(new Runnable() {
	    	long current = System.currentTimeMillis();
			public void run() {
				logger.info("thread start...");
				Exception ex = null;
				while (!closeFlag) {
					//logger.info("进入循环体里面...");
					if(System.currentTimeMillis()-current>=10*1000){
						current = System.currentTimeMillis();
						logger.info("write data to kafka,total fail num="+p.getTotalFailCount()
								+",getContinuityFailCount="+p.getContinuityFailCount());
					}
					if(p.getContinuityFailCount()>=Utils.getValue(userConfig, Utils.KAFKA_WRITE_FAIL_THRESHOLD, 5)){
						suspend(); 
					}else{
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				closeable = true;
			}
	    });
	    executor.start();
	  
    }

    /**
     * 判断topic是否存在，有就把json发送到这个topic，没有就发送到DEFAULT_UNKNOWN_TOPIC（同时把这个未知的topic也加入到了json里面）
     */
    public void execute(Tuple input) {
    	
    	String topic = input.getStringByField("topic");
    	String _dfp_ = input.getStringByField("_dfp_");
    	String json = input.getStringByField("json");
 
    	//在异步方法send里面进行collector方法的调用
    	p.produce(topic,_dfp_,json,input);
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declare) {
    	declare.declareStream(Utils.SUCCESS_STREAM, new Fields("topic","message","exception","json"));
		declare.declareStream(Utils.KAFKA_WRITE_FAIL_STREAM, new Fields("topic","message","exception","json"));
                 
    }
    
    /**
     * 当topology出现RuntimeException时，会关闭当前的worker，此时可能调用了cleanup方法（没确认）
     */
	public void cleanup() {
		closeFlag = true;
		logger.info("clean up...");
		while (!closeable) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				logger.info(e);
			}
		}
	}
	
}
