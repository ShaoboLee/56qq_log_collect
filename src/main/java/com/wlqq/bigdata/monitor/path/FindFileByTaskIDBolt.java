package com.wlqq.bigdata.monitor.path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;

import com.wlqq.bigdata.jdbc.HiveJDBC;
import com.wlqq.bigdata.utils.HdfsUtils;
import com.wlqq.bigdata.utils.Utils;

public class FindFileByTaskIDBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(FindFileByTaskIDBolt.class);
	OutputCollector collector;
	private Map<String, Object> userConfig;
	private FileSystem fs;
	private String fsUrl;
	private String hivehost;
	private String hiveport;
	private HiveJDBC hiveJDBC;
	private Map<String,String> taskID2Table;//<taskid,tablename>
	private Map<String,String> taskID2DayPartition;//<taskid,day partition name>
	private String database;
	private String hdfsWritePath; 
	private String hiveJsonSerdeJarPath;
	
	public FindFileByTaskIDBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
		hivehost = Utils.getValue(userConfig, Utils.HIVE_HOST, "v29");
    	hiveport = Utils.getValue(userConfig, Utils.HIVE_PORT, "10000");
    	hdfsWritePath = Utils.getValue(userConfig, Utils.HDFS_WRITE_PATH,"");
    	hiveJsonSerdeJarPath = Utils.getValue(userConfig, Utils.HIVE_JSON_SERDE_JAR_PATH, "");
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.database = Utils.getValue(userConfig, Utils.HIVE_DATABASE, "client_business_log");
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
		
		taskID2Table = new HashMap<String,String>();
		taskID2DayPartition = new HashMap<String,String>();
		
    	updateTables(database);
    	
    	
	}

	/**
	 * 获取database下面的所有表名，然后找出符合规则的名称，从中抽取出业务id，存放到taskID2Table<业务id，table名称>里面,
	 * 再从符合规则的里面找出每个表对应的分区名称，存放到taskID2DayPartition<业务id，分区名称>
	 * @param database
	 */
	private void updateTables(String database){
		
		hiveJDBC = new HiveJDBC();
    	boolean flag = hiveJDBC.init(hivehost, hiveport);//init
    	
    	if(!flag){
    		logger.error("hive jdbc connect failed");
    		return;
    	}
		List<String> list = hiveJDBC.showTables(database);
		System.out.println(list);
    	//Pattern pattern = Pattern.compile("(\\d+)_[dh]$");
		Pattern pattern = Pattern.compile("([^_]+)_[dh]$");
    	
    	for(String t:list){
    		Matcher matcher = pattern.matcher(t);
    		if(matcher.find()){//找到符合匹配的表
    			String taskId = matcher.group(1);
    			taskID2Table.put(taskId, t);
    			String dayName = hiveJDBC.getPartition(database,t,hiveJsonSerdeJarPath);
    			logger.info("taskid="+taskId+"dayName="+dayName);
    			taskID2DayPartition.put(taskId, dayName);
    		}
    	}
    	hiveJDBC.close();
	}
	
	
	/**
	 * 压缩类的名称必须正确，否则会捕获异常直接退出
	 */
	public void execute(Tuple input) {
		
   	long begin = System.currentTimeMillis();
    	//在topic和hive的表名做映射时，先对topic做处理，改小写和替换掉-和_
		String taskId = input.getString(0);
		//String tableId = input.getString(0).toLowerCase().replaceAll("[_\\-]", "");
		String logFile = hdfsWritePath+"/"+taskId;
		String tablename = taskID2Table.get(taskId.toLowerCase().replaceAll("[_\\-]", ""));
		if(tablename==null){//可能是新增加的业务，之前没有建这个表
			updateTables(database);
			tablename = taskID2Table.get(taskId.toLowerCase().replaceAll("[_\\-]", ""));
			if(tablename==null){
				logger.error("业务id:"+taskId+",对应的表在hive里面还没有建立,也有可能hiveserver挂掉了，导致show tables操作失败");
				collector.ack(input);
				return;
			}
		}
		
		String partitionName = taskID2DayPartition.get(taskId.toLowerCase().replaceAll("[_\\-]", ""));
		if(partitionName==null){
			logger.error("业务id:"+taskId+",查找table:"+tablename+"对应的分区名称失败");
			collector.ack(input);
			return;
		}
		String[] pn = partitionName.split(",");
		String dayPartitonName = pn[0];
		String dayFormat = "yyyy-MM-dd";
		SimpleDateFormat df = new SimpleDateFormat(dayFormat); 
		String commonPath = Utils.getValue(userConfig, Utils.HIVE_DB_PATH, "")+"/"+database+".db"+"/"+tablename;	
		logger.info("monitor path:"+logFile);
		
		try {
			Path logPath = new Path(logFile); 
			FileStatus[] fstatus = fs.listStatus(logPath);
			String[] ar = logFile.split("/");
			String id = ar[ar.length-1];
			
			Map<String, TreeMap> threadMap = new HashMap<String,TreeMap>();//<thread_id,<unixtime,file_name>>
			Map<String,Map> fileSizeMap = new HashMap<String,Map>();//<thread_id,<file_name,file_size>>
			//用于存放那些已经存在的压缩文件，但生成时间小于5分钟的文件，后续处理中暂时不处理这种文件，因为有可能还在压缩中，
			//如果执行删除，会出现hdfs的异常错误，可能造成压缩文件的最后一行出问题,超过5分钟的就删除
			Set<String> compressSet = new HashSet<String>();
			
			for(FileStatus f:fstatus){
				if(f.isFile()){
					String fl = f.getPath().getName();
					long filetime = f.getModificationTime();
					logger.info("file="+fl+",time:"+filetime);
					if(!fl.startsWith(id)){//无效的文件
						continue;
					}
					String threadID = fl.split("\\-")[fl.split("\\-").length-3];//executor的id
					String unixtime = fl.split("\\-")[fl.split("\\-").length-1].split("\\.")[0];//文件创建时的时间戳
					
					if(!fl.endsWith("txt") && System.currentTimeMillis()-filetime<5*60*1000){//压缩文件，存在时间小于5分钟
						logger.info(logFile+"/"+fl+" 可能正在压缩中...");
						compressSet.add(fl.split("\\.")[0]);
						continue;
					}
					
					if(!fl.endsWith("txt") && System.currentTimeMillis()-Long.parseLong(unixtime)>=5*60*1000){//压缩文件，存在时间大于5分钟，删除
						logger.info(logFile+"/"+fl+" 已经存在超过了5分钟，删除");
						HdfsUtils.delete(fs, logFile+"/"+fl);
						continue;
					}
					
					if(threadMap.containsKey(threadID)){
						threadMap.get(threadID).put(unixtime, fl);
						fileSizeMap.get(threadID).put(fl, f.getLen());
					}else{
						TreeMap tm = new TreeMap<String, String>();
						tm.put(unixtime, fl);
						threadMap.put(threadID, tm);
						Map<String,Long> mp = new HashMap<String,Long>();
						mp.put(fl, f.getLen());
						fileSizeMap.put(threadID, mp);
					}
				}
			}	
			
			for (Entry<String, TreeMap> entry : threadMap.entrySet()){
				TreeMap<String,String> tm = entry.getValue();
				String threadID = entry.getKey();
				Map<String,Long> sizeMap = fileSizeMap.get(threadID);
				Iterator it = tm.keySet().iterator();
				String last = null;
				while(it.hasNext()){//每一个executor的id进行分组
					if(last==null){
						last = it.next().toString();
					}else{//处理已经结束的文件
						//deal last
						long filesize = sizeMap.get(tm.get(last));
						if(filesize==0 || !tm.get(last).endsWith(".txt")){//文件大小为0或者是压缩文件
							HdfsUtils.delete(fs, logFile+"/"+tm.get(last));
							last = it.next().toString();
							continue;
						}
						
						if(compressSet.contains(tm.get(last).split("\\.")[0])){//已经存在压缩文件了，暂时不处理
							logger.info(logFile+"/"+tm.get(last)+" 暂时不处理");
							last = it.next().toString();
							continue;
						}
						
						findValidFile(input,last, commonPath, dayPartitonName, tablename, tm, df, logFile);
						last = it.next().toString();
					}
				}
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		collector.ack(input);
		long end = System.currentTimeMillis();
    	logger.info("cost "+(end-begin)+" ms.");
	}
	
	/**
	 * 首先判断在hdfs存放hive表的目录下查看是否已经存在该文件，存在就把storm存放临时滚动文件的目录下删除该文件，否则就发往下一级bolt
	 * 对该文件进行压缩，然后load到hive
	 * @param input
	 * @param unixtime 文件名里面包含的时间戳
	 * @param commonPath 该记录对应的hive表在hdfs里面的位置
	 * @param dayPartitonName 日分区名称
	 * @param tablename hive表名
	 * @param tm 时间戳和文件名的映射
	 * @param df
	 * @param logFile 滚动文件在hdfs里面的存放目录
	 */
	public void findValidFile(Tuple input,String unixtime,String commonPath,String dayPartitonName
			,String tablename,TreeMap<String,String> tm,SimpleDateFormat df,String logFile){
		String day = df.format(new Date(Long.parseLong(unixtime))); 
		String directory = commonPath+"/"+dayPartitonName+"="+day;
		String name = tm.get(unixtime).split("\\.")[0];
		String filePath = logFile+"/"+tm.get(unixtime);
		String sql = "";
		if(contain(directory,name)){//在hive的表分区下面已经存在
			logger.info("file has exists in hive's table");
			HdfsUtils.delete(fs, filePath);
		}else{//load file to hive
			collector.emit(input,new Values(filePath,tablename,dayPartitonName,day));
		}
	}
	
	/**
	 * 判断在hdfs的directory目录下，是否已经包含了name的文件（不考虑后缀）
	 * @param directory
	 * @param name
	 * @return
	 */
	public boolean contain(String directory,String name){//判断在hive表对应分区下面是否已经包含了该文件
		try {
			FileStatus[] fstatus = fs.listStatus(new Path(directory));
			for(FileStatus f:fstatus){
				String _name = f.getPath().getName();
				if(_name.startsWith(name)){
					logger.error("file:"+name+" has bean existed...");
					return true;
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("filePath","tablename","dayPartitonName","day"));
	}
	
//	public static void main(String[] args){
//		System.out.println("ADBD-DDB_DTTC-G".toLowerCase().replaceAll("[_\\-]", ""));
//		FindFileByTaskIDBolt fb = new FindFileByTaskIDBolt(new HashMap<String, Object>());
//		fb.hiveJsonSerdeJarPath="hdfs://c1/apps/hive/udfjars/json-serde-1.3.8-SNAPSHOT-jar-with-dependencies.jar";
//		fb.taskID2Table = new HashMap<String,String>();
//		fb.taskID2DayPartition = new HashMap<String,String>();
//		fb.updateTables("client_business_log");
//		System.out.println(fb.taskID2Table);
//		System.out.println(fb.taskID2DayPartition);
//	}
}


