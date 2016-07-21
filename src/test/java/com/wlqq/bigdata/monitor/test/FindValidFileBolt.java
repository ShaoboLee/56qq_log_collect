package com.wlqq.bigdata.monitor.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.wlqq.bigdata.utils.HdfsUtils;
import com.wlqq.bigdata.utils.Utils;

public class FindValidFileBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(FindValidFileBolt.class);
	OutputCollector collector;
	Map<String, Object> userConfig;
	FileSystem fs;
	String fsUrl;
	SimpleDateFormat dfHour;
	
	public FindValidFileBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
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
	 * ѹ��������Ʊ�����ȷ������Ჶ���쳣ֱ���˳�
	 */
	public void execute(Tuple input) {
		
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
		
		String logFile = arr[0];
		int interval = Integer.parseInt(arr[1]);
		String database = arr[2];
		String tablename = arr[3];
		String dayPartitonName = arr[4];
		String dayFormat = arr[5];
		String hourPartitionName = arr[6];
		String hiveFileFormat = arr[7];
		String compressClass = arr[8];
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
			
			for(FileStatus f:fstatus){
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
				while(it.hasNext()){//ÿһ��executor��id���з���
					if(last==null){
						last = it.next().toString();
					}else{//�����Ѿ��������ļ�
						//deal last
						long filesize = sizeMap.get(tm.get(last));
						if(filesize==0 || !tm.get(last).endsWith(".txt")){//�ļ���СΪ0������ѹ���ļ�
							HdfsUtils.delete(fs, logFile+"/"+tm.get(last));
							last = it.next().toString();
							continue;
						}
						findValidFile(input,last, commonPath, dayPartitonName, interval, hourPartitionName, database, tablename, 
								tm, df, logFile, compressClass,hiveFileFormat);
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
		long end = time.milliseconds();
    	logger.info("cost "+(end-begin)+" ms.");
	}
	
	public void findValidFile(Tuple input,String unixtime,String commonPath,String dayPartitonName,int interval,String hourPartitionName
			,String database,String tablename,TreeMap<String,String> tm,SimpleDateFormat df,String logFile,String compressClass
			,String hiveFileFormat){
		String hour = dfHour.format(new Date(Long.parseLong(unixtime)));
		int partition = Integer.parseInt(hour)/interval*interval;//�ҵ���Ӧ��Сʱ����ʱ��
		String hourPartition = partition<10?"0"+partition:partition+"";
		String day = df.format(new Date(Long.parseLong(unixtime))); 
		//��ʱ��Сʱ����ʱ��ע��interval!=1�����
		String directory = commonPath+"/"+dayPartitonName+"="+day+(interval==24?"":"/"+hourPartitionName+"="+hourPartition);
		String name = tm.get(unixtime).split("\\.")[0];
		String filePath = logFile+"/"+tm.get(unixtime);
		String sql = "";
		if(contain(directory,name)){//��hive�ı���������Ѿ�����
			logger.info("file has exists in hive's table");
			HdfsUtils.delete(fs, filePath);
		}else{//load file to hive
			collector.emit(input,new Values(filePath,interval,database,tablename,dayPartitonName,hour,day,
					hourPartitionName,compressClass,hiveFileFormat));
		}
	}
	
	public boolean contain(String directory,String name){//�ж���hive���Ӧ���������Ƿ��Ѿ������˸��ļ�
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
		// TODO Auto-generated method stub
		//collector.emit(new Values(filePath,newPath,interval,database,tablename,dayPartitonName,hour,day,hourPartitionName,input));
		declarer.declare(new Fields("filePath","interval","database","tablename","dayPartitonName","hour","day","hourPartitionName",
				"compressClass","hiveFileFormat"));
	}
	
	public static void main(String[] args){
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/monitor.yaml":args[0], true);
		MonitorBolt mb = new MonitorBolt(userConfig);
		mb.prepare(null, null, null);
		mb.execute(null);
		
	}

}

