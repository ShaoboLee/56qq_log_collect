package com.wlqq.bigdata.monitor.path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.wlqq.bigdata.jdbc.HiveJDBC;
import com.wlqq.bigdata.utils.Utils;

public class MonitorDirectorySpout extends BaseRichSpout{
	
	private static final Log logger = LogFactory.getLog(MonitorDirectorySpout.class);
	
	private Map<String, Object> userConfig;
	private SpoutOutputCollector collector;
	private FileSystem fs;
	private String fsUrl;
//	private String hivehost;
//	private String hiveport;
//	private String hiveJsonSerdeJarPath;
//	private HiveJDBC hiveJDBC;
	private String hdfsWritePath; 
	
	public MonitorDirectorySpout(Map<String, Object> userConfig){
		this.userConfig = userConfig;
//		hivehost = Utils.getValue(userConfig, Utils.HIVE_HOST, "v29");
//    	hiveport = Utils.getValue(userConfig, Utils.HIVE_PORT, "10000");
//    	hiveJsonSerdeJarPath = Utils.getValue(userConfig, Utils.HIVE_JSON_SERDE_JAR_PATH, "");
    	hdfsWritePath = Utils.getValue(userConfig, Utils.HDFS_WRITE_PATH,"");
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		Configuration hdfsConfig = new Configuration();
		fsUrl = Utils.getValue(userConfig, Utils.HDFS_URL, "");
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

//	Pattern pattern = Pattern.compile("^\\d+$");
//	
//	private boolean isNum(String num){
//		Matcher matcher = pattern.matcher(num);
//		return matcher.matches();
//	}
	
    /**
     * 访问hdfs的hdfsWritePath目录，如果文件是目录并且是数值型（业务id）就往下一级bolt发送	
     */
	public void nextTuple() {

		if("".equals(hdfsWritePath)){
			logger.error("没有配写入hdfs的临时目录");
			return;
		}
		Path logPath = new Path(hdfsWritePath); 
		try {
			FileStatus[] fstatus = fs.listStatus(logPath);
			for(FileStatus f:fstatus){
				String num = f.getPath().getName();
				//if(f.isDirectory() && isNum(num)){
				if(f.isDirectory()){
					logger.info("monitor taskId:"+num);
					collector.emit(new Values(num));
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		int sleepSec = Utils.getValue(userConfig, Utils.SPOUT_SEND_INTERVAL_SEC, 60);
		try {
			logger.info("sleep "+sleepSec+"s");
			Thread.currentThread().sleep(sleepSec*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("taskId"));
		
	}

}

