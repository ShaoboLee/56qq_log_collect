package com.wlqq.bigdata.monitor.path;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift7.TException;

import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.NimbusClient;

import com.wlqq.bigdata.jdbc.HiveJDBC;
import com.wlqq.bigdata.utils.HdfsUtils;
import com.wlqq.bigdata.utils.Utils;

public class CompressAndLoadBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(CompressAndLoadBolt.class);
	private OutputCollector collector;
	private Map<String, Object> userConfig;
	private FileSystem fs;
	private String fsUrl;
	private String hivehost;
	private String hiveport;
	private String hiveJsonSerdeJarPath;
	private HiveJDBC hiveJDBC;
	private String database;
	private String compressClass;

	public CompressAndLoadBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
		hivehost = Utils.getValue(userConfig, Utils.HIVE_HOST, "v29");
    	hiveport = Utils.getValue(userConfig, Utils.HIVE_PORT, "10000");
    	hiveJsonSerdeJarPath = Utils.getValue(userConfig, Utils.HIVE_JSON_SERDE_JAR_PATH, "");
    	database = Utils.getValue(userConfig, Utils.HIVE_DATABASE, "");
    	compressClass = Utils.getValue(userConfig, Utils.HIVE_FILE_COMPRESS_CLASS, "");
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		Configuration hdfsConfig = new Configuration();
		fsUrl = Utils.getValue(userConfig, Utils.HDFS_URL, "");
        if(userConfig != null){
            for(String key : userConfig.keySet()){//namenode HA
                hdfsConfig.set(key, String.valueOf(userConfig.get(key)));
            }
        }
		try {
			fs = FileSystem.get(URI.create(fsUrl), hdfsConfig);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("create FS fail",e);
		}
		
		
	}

	public void execute(Tuple input) {
		
		hiveJDBC = new HiveJDBC();
    	boolean flag = hiveJDBC.init(hivehost, hiveport);//init
    	
    	if(!flag){
    		logger.error("hive jdbc connect failed");
    		collector.ack(input);
    		return;
    	}

    	long begin = System.currentTimeMillis();
    	
		String filePath = input.getStringByField("filePath");
		String tablename = input.getStringByField("tablename");
		String dayPartitonName = input.getStringByField("dayPartitonName");
		String day = input.getStringByField("day");
		Class<?> codecClass = null;
		
		try {
			codecClass = Class.forName(compressClass);
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			logger.error("compressClass is wrong,compressClass="+compressClass,e1);
			collector.ack(input);
			return;
		}
		
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, fs.getConf());
		String newPath = filePath.replaceAll("\\.txt", codec.getDefaultExtension());
		
		try {
			HdfsUtils.compress(fs, codec, new Path(filePath), new Path(newPath),true);
			String sql = "load data inpath '"+newPath+"' into table "+database+"."+tablename+" partition("+dayPartitonName+"="+"'"+day+"'"+")";
			logger.info("excute sql--->"+sql);
	    	
			if(load(sql)){//load comprees file
				logger.info("excute sql success...");
				HdfsUtils.delete(fs, filePath);
			}
		    
		} catch (Exception e) {//只导入压缩文件
			e.printStackTrace();
		    HdfsUtils.delete(fs, newPath);
			
		}
		collector.ack(input);
		hiveJDBC.close();
		logger.info("compress and load cost "+(System.currentTimeMillis()-begin)+" ms.");
		
	}
	
	public boolean load(String sql){
		
	    String addjar = "add jar "+hiveJsonSerdeJarPath;
	    hiveJDBC.loadData(addjar);
	    	
	    boolean success = hiveJDBC.loadData(sql);
	    return success;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}

