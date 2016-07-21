package com.wlqq.bigdata.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import com.wlqq.bigdata.jdbc.HiveJDBC;
import com.wlqq.bigdata.utils.HdfsUtils;
import com.wlqq.bigdata.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * 
 * 1、配置里面的参数必须正确，否则退出处理
 * 2、压缩类必须正确，否则也退出
 * 3、处理逻辑：
 * 1）遍历指定目录下面的文件，按executor的id进行分组，相同的在一起，再按文件生成的时间戳进行排序
 * 2）将每一组里面除了最后一个文件外（可能在写入中）的其他文件都压缩，然后load到hive里面
 * 3）对于最后一个文件，计算它需要load到hive的时间点，如何当前时间超过了这个时间点一段时间（2分钟，可配置），就把它压缩在load到hive
 * 
 * @author wangchi
 *
 */
public class MonitorBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(MonitorBolt.class);
	OutputCollector collector;
	Map<String, Object> userConfig;
	FileSystem fs;
	String fsUrl;
	String hivehost;
	String hiveport;
	String hiveJsonSerdeJarPath;
	SimpleDateFormat dfHour;
	HiveJDBC hiveJDBC;
	
	public MonitorBolt(Map<String, Object> userConfig){
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

	/**
	 * 压缩类的名称必须正确，否则会捕获异常直接退出
	 */
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
		hiveJDBC = new HiveJDBC();
    	boolean flag = hiveJDBC.init(hivehost, hiveport);//init
    	
    	if(!flag){
    		logger.error("hive jdbc connect failed");
    		collector.ack(input);
    		return;
    	}
		
		Time time = new SystemTime();
    	long begin = time.milliseconds();
		String para = input.getString(0);
		//String para = "/storm/10001,24,test,t10001_d,dt,yyyy-MM-dd,,json,org.apache.hadoop.io.compress.SnappyCodec";
		String[] arr = para.split(",");
		if(arr.length!=9){
			logger.error("para is wrong,para="+para);
			collector.ack(input);
			return;
		}
		
		SimpleDateFormat df = new SimpleDateFormat(Utils.getValue(userConfig, Utils.HIVE_PARTITION_DAY_FORMAT, "yyyy-MM-dd")); 
		
		String logFile = arr[0];
		int interval = Integer.parseInt(arr[1]);
		String database = arr[2];
		String tablename = arr[3];
		String dayPartitonName = arr[4];
		String dayFormat = arr[5];
		String hourPartitionName = arr[6];
		String hiveFileFormat = arr[7];
		String compressClass = arr[8];
		String commonPath = Utils.getValue(userConfig, Utils.HIVE_DB_PATH, "")+"/"+database+".db"+"/"+tablename;	
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
		
		try {
			Path logPath = new Path(logFile); 
			FileStatus[] fstatus = fs.listStatus(logPath);
			String[] ar = logFile.split("/");
			String id = ar[ar.length-1];
			
			Map<String, TreeMap> threadMap = new HashMap<String,TreeMap>();//<thread_id,<unixtime,file_name>>
			Map<String,Map> fileSizeMap = new HashMap<String,Map>();//<thread_id,<file_name,file_size>>
			
			for(FileStatus f:fstatus){
				//if(f.isFile() && f.getLen()>0 && f.getPath().getName().endsWith(".txt")){
				if(f.isFile()){
					String fl = f.getPath().getName();
					if(!fl.startsWith(id)){
						continue;
					}
					String threadID = fl.split("\\-")[fl.split("\\-").length-3];
					String unixtime = fl.split("\\-")[fl.split("\\-").length-1].split("\\.")[0];
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
				while(it.hasNext()){
					if(last==null){
						last = it.next().toString();
					}else{//处理已经结束的文件
						//deal last
						long filesize = sizeMap.get(tm.get(last));
						if(filesize==0 || !tm.get(last).endsWith(".txt")){//文件大小为0或者是压缩文件
//							if(System.currentTimeMillis()-Long.parseLong(last)>2*60*1000){
//								HdfsUtils.delete(fs, logFile+"/"+tm.get(last));
//							}
							HdfsUtils.delete(fs, logFile+"/"+tm.get(last));
							last = it.next().toString();
							continue;
						}
						
//						if(System.currentTimeMillis()-Long.parseLong(last)>2*60*1000){//防止文件滚动时候，出现多个文件，导致错误
//							createSqlAndLoad(last, commonPath, dayPartitonName, interval, hourPartitionName, database, tablename, 
//									tm, df, logFile, codec, hiveFileFormat);
//						}
						createSqlAndLoad(last, commonPath, dayPartitonName, interval, hourPartitionName, database, tablename, 
								tm, df, logFile, codec, hiveFileFormat);
						last = it.next().toString();
					}
				}
				
				
				long filesize = sizeMap.get(tm.get(last));
				if(filesize==0 || !tm.get(last).endsWith(".txt")){//文件大小为0或者是压缩文件
					continue;//暂时不处理
				}
				
				//处理每个executor生成的最后一个文件
				long currentUnixtime = System.currentTimeMillis();
				String sql = null;
				int hiveLoadDelay = Utils.getValue(userConfig, Utils.HIVE_LOAD_DELAY_MINUTE, 5);
				Calendar cl = Calendar.getInstance();
				cl.setTime(new Date(Long.parseLong(last)));
				boolean load = false;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				if(interval==24){//day
					cl.add(Calendar.DAY_OF_YEAR,1);
					Date d = cl.getTime();
					d.setHours(hiveLoadDelay/60);
					d.setMinutes(hiveLoadDelay%60);
					long delayRotationTime = d.getTime();//找到该文件最迟应该load到hive的时间
					logger.info("file:"+logFile+"/"+tm.get(last)+" will load to hive at time "+sdf.format(new Date(d.getTime())));
					if(currentUnixtime>=delayRotationTime){
						load = true;
					}
				}else{//hour
					//注意hour!=1的情况下，计算下次应该load到hive的时间
					int hour = cl.getTime().getHours();
					int nextHour = hour/interval*interval+interval>=24?24:hour/interval*interval+interval;
					cl.add(Calendar.HOUR_OF_DAY,nextHour-hour+hiveLoadDelay/60);
					Date d = cl.getTime();
					d.setMinutes(hiveLoadDelay%60);
					long delayRotationTime = d.getTime();//找到该文件最迟应该load到hive的时间
					logger.info("file:"+logFile+"/"+tm.get(last)+" will load to hive at time "+sdf.format(new Date(d.getTime())));
					if(currentUnixtime>=delayRotationTime){
						load =true;
					}
				}
				if(load){
					createSqlAndLoad(last, commonPath, dayPartitonName, interval, hourPartitionName, database, tablename, 
							tm, df, logFile, codec, hiveFileFormat);
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
		hiveJDBC.close();
		long end = time.milliseconds();
    	logger.info("cost "+(end-begin)+" ms.");
	}
	
	public void createSqlAndLoad(String unixtime,String commonPath,String dayPartitonName,int interval,String hourPartitionName
			,String database,String tablename,TreeMap<String,String> tm,SimpleDateFormat df,String logFile,CompressionCodec codec
			,String hiveFileFormat){
		String hour = dfHour.format(new Date(Long.parseLong(unixtime)));
		int partition = Integer.parseInt(hour)/interval*interval;//找到对应的小时分区时间
		String hourPartition = partition<10?"0"+partition:partition+"";
		String day = df.format(new Date(Long.parseLong(unixtime))); 
		//当时按小时分区时，注意interval!=1的情况
		String directory = commonPath+"/"+dayPartitonName+"="+day+(interval==24?"":"/"+hourPartitionName+"="+hourPartition);
		String name = tm.get(unixtime).split("\\.")[0];
		String filePath = logFile+"/"+tm.get(unixtime);
		String sql = "";
		if(contain(directory,name)){//在hive的表分区下面已经存在
			logger.info("file has exists in hive's table");
			HdfsUtils.delete(fs, filePath);
		}else{//load file to hive
			String newPath = filePath.replaceAll("\\.txt", codec.getDefaultExtension());
			try {
				HdfsUtils.compress(fs, codec, new Path(filePath), new Path(newPath),true);
				sql = createSql(interval, newPath, database, tablename, dayPartitonName
						, hour, day, hourPartitionName);
				logger.info("excute sql--->"+sql);
		    	
				if(load(hiveFileFormat, sql)){//load comprees file
					logger.info("excute sql success...");
					HdfsUtils.delete(fs, filePath);
				}
			    
			} catch (Exception e) {//只导入压缩文件
				// TODO Auto-generated catch block
				e.printStackTrace();
//				sql = createSql(interval, filePath, database, tablename, dayPartitonName
//						, hour, day, hourPartitionName);
//				logger.info("excute sql--->"+sql);
//				load(hiveFileFormat, sql);
			    HdfsUtils.delete(fs, newPath);
				
			}
		}
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
//		HiveJDBC hiveJDBC = new HiveJDBC();
//	    hiveJDBC.init(hivehost, hiveport);//init
	    	
	    if(hiveFileFormat.equalsIgnoreCase("json")){//add jar 
	    		String addjar = "add jar "+hiveJsonSerdeJarPath;
	    	hiveJDBC.loadData(addjar);
	    }
	    	
	    boolean success = hiveJDBC.loadData(sql);
	    //hiveJDBC.close();
	    return success;
	}
	
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
			e.printStackTrace();
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
		// TODO Auto-generated method stub
		
	}
	
	public static void main(String[] args){
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/monitor.yaml":args[0], true);
		MonitorBolt mb = new MonitorBolt(userConfig);
		mb.prepare(null, null, null);
		mb.execute(null);
		
	}

}
