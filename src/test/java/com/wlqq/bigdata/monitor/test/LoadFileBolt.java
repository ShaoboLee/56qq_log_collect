package com.wlqq.bigdata.monitor.test;

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

import com.wlqq.bigdata.common.HdfsUtils;
import com.wlqq.bigdata.common.Utils;
import com.wlqq.bigdata.jdbc.HiveJDBC;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class LoadFileBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(LoadFileBolt.class);
	OutputCollector collector;
	Map<String, Object> userConfig;
	FileSystem fs;
	String fsUrl;
	String hivehost;
	String hiveport;
	String hiveJsonSerdeJarPath;
	SimpleDateFormat dfHour;
	HiveJDBC hiveJDBC;

	public LoadFileBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
		hivehost = Utils.getValue(userConfig, Utils.HIVE_HOST, "v29");
    	hiveport = Utils.getValue(userConfig, Utils.HIVE_PORT, "10000");
    	hiveJsonSerdeJarPath = Utils.getValue(userConfig, Utils.HIVE_JSON_SERDE_JAR_PATH, "");
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		Configuration hdfsConfig = new Configuration();
		fsUrl = Utils.getValue(userConfig, Utils.HDFS_URL, "");
		dfHour = new SimpleDateFormat("HH");
        if(userConfig != null){
            for(String key : userConfig.keySet()){
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
		int interval = input.getIntegerByField("interval");
		String database = input.getStringByField("database");
		String tablename = input.getStringByField("tablename");
		String dayPartitonName = input.getStringByField("dayPartitonName");
		String hour = input.getStringByField("hour");
		String day = input.getStringByField("day");
		String hourPartitionName = input.getStringByField("hourPartitionName");
		String compressClass = input.getStringByField("compressClass");
		String hiveFileFormat = input.getStringByField("hiveFileFormat");
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
			String sql = createSql(interval, newPath, database, tablename, dayPartitonName
					, hour, day, hourPartitionName);
			logger.info("excute sql--->"+sql);
	    	
			if(load(hiveFileFormat, sql)){//load comprees file
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
	
	public String createSql(int interval,String newPath,String database,String tablename,String dayPartitonName
			,String hour,String day,String hourPartitionName){
		String sql = null;
		if(interval==24){
    		sql = "load data inpath '"+newPath+"' into table "+database+"."+tablename+" partition("+dayPartitonName+"="+"'"+day+"'"+")";
    	}else{//当interval!=1时，需要注意这个文件应该插入的小时分区
    		int partition = Integer.parseInt(hour)/interval*interval;
    		String hourPartition = partition<10?"0"+partition:partition+"";
    		sql = "load data inpath '"+newPath+"' into table "+database+"."+tablename
    				+" partition("+dayPartitonName+"="+"'"+day+"'"+","+hourPartitionName+"="+"'"+hourPartition+"'"+")";
    	}
		return sql;
	}
	
	public boolean load(String hiveFileFormat,String sql){
		
	    if(hiveFileFormat.equalsIgnoreCase("json")){//add jar 
	    		String addjar = "add jar "+hiveJsonSerdeJarPath;
	    	hiveJDBC.loadData(addjar);
	    }
	    	
	    boolean success = hiveJDBC.loadData(sql);
	    return success;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("topic","_dfp_","json"));
	}

}
