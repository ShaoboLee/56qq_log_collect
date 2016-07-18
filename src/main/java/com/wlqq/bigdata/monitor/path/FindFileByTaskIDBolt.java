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
	 * ��ȡdatabase��������б�����Ȼ���ҳ����Ϲ�������ƣ����г�ȡ��ҵ��id����ŵ�taskID2Table<ҵ��id��table����>����,
	 * �ٴӷ��Ϲ���������ҳ�ÿ�����Ӧ�ķ������ƣ���ŵ�taskID2DayPartition<ҵ��id����������>
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
    		if(matcher.find()){//�ҵ�����ƥ��ı�
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
	 * ѹ��������Ʊ�����ȷ������Ჶ���쳣ֱ���˳�
	 */
	public void execute(Tuple input) {
		
   	long begin = System.currentTimeMillis();
    	//��topic��hive�ı�����ӳ��ʱ���ȶ�topic��������Сд���滻��-��_
		String taskId = input.getString(0);
		//String tableId = input.getString(0).toLowerCase().replaceAll("[_\\-]", "");
		String logFile = hdfsWritePath+"/"+taskId;
		String tablename = taskID2Table.get(taskId.toLowerCase().replaceAll("[_\\-]", ""));
		if(tablename==null){//�����������ӵ�ҵ��֮ǰû�н������
			updateTables(database);
			tablename = taskID2Table.get(taskId.toLowerCase().replaceAll("[_\\-]", ""));
			if(tablename==null){
				logger.error("ҵ��id:"+taskId+",��Ӧ�ı���hive���滹û�н���,Ҳ�п���hiveserver�ҵ��ˣ�����show tables����ʧ��");
				collector.ack(input);
				return;
			}
		}
		
		String partitionName = taskID2DayPartition.get(taskId.toLowerCase().replaceAll("[_\\-]", ""));
		if(partitionName==null){
			logger.error("ҵ��id:"+taskId+",����table:"+tablename+"��Ӧ�ķ�������ʧ��");
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
			//���ڴ����Щ�Ѿ����ڵ�ѹ���ļ���������ʱ��С��5���ӵ��ļ���������������ʱ�����������ļ�����Ϊ�п��ܻ���ѹ���У�
			//���ִ��ɾ���������hdfs���쳣���󣬿������ѹ���ļ������һ�г�����,����5���ӵľ�ɾ��
			Set<String> compressSet = new HashSet<String>();
			
			for(FileStatus f:fstatus){
				if(f.isFile()){
					String fl = f.getPath().getName();
					long filetime = f.getModificationTime();
					logger.info("file="+fl+",time:"+filetime);
					if(!fl.startsWith(id)){//��Ч���ļ�
						continue;
					}
					String threadID = fl.split("\\-")[fl.split("\\-").length-3];//executor��id
					String unixtime = fl.split("\\-")[fl.split("\\-").length-1].split("\\.")[0];//�ļ�����ʱ��ʱ���
					
					if(!fl.endsWith("txt") && System.currentTimeMillis()-filetime<5*60*1000){//ѹ���ļ�������ʱ��С��5����
						logger.info(logFile+"/"+fl+" ��������ѹ����...");
						compressSet.add(fl.split("\\.")[0]);
						continue;
					}
					
					if(!fl.endsWith("txt") && System.currentTimeMillis()-Long.parseLong(unixtime)>=5*60*1000){//ѹ���ļ�������ʱ�����5���ӣ�ɾ��
						logger.info(logFile+"/"+fl+" �Ѿ����ڳ�����5���ӣ�ɾ��");
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
						
						if(compressSet.contains(tm.get(last).split("\\.")[0])){//�Ѿ�����ѹ���ļ��ˣ���ʱ������
							logger.info(logFile+"/"+tm.get(last)+" ��ʱ������");
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
	 * �����ж���hdfs���hive���Ŀ¼�²鿴�Ƿ��Ѿ����ڸ��ļ������ھͰ�storm�����ʱ�����ļ���Ŀ¼��ɾ�����ļ�������ͷ�����һ��bolt
	 * �Ը��ļ�����ѹ����Ȼ��load��hive
	 * @param input
	 * @param unixtime �ļ������������ʱ���
	 * @param commonPath �ü�¼��Ӧ��hive����hdfs�����λ��
	 * @param dayPartitonName �շ�������
	 * @param tablename hive����
	 * @param tm ʱ������ļ�����ӳ��
	 * @param df
	 * @param logFile �����ļ���hdfs����Ĵ��Ŀ¼
	 */
	public void findValidFile(Tuple input,String unixtime,String commonPath,String dayPartitonName
			,String tablename,TreeMap<String,String> tm,SimpleDateFormat df,String logFile){
		String day = df.format(new Date(Long.parseLong(unixtime))); 
		String directory = commonPath+"/"+dayPartitonName+"="+day;
		String name = tm.get(unixtime).split("\\.")[0];
		String filePath = logFile+"/"+tm.get(unixtime);
		String sql = "";
		if(contain(directory,name)){//��hive�ı���������Ѿ�����
			logger.info("file has exists in hive's table");
			HdfsUtils.delete(fs, filePath);
		}else{//load file to hive
			collector.emit(input,new Values(filePath,tablename,dayPartitonName,day));
		}
	}
	
	/**
	 * �ж���hdfs��directoryĿ¼�£��Ƿ��Ѿ�������name���ļ��������Ǻ�׺��
	 * @param directory
	 * @param name
	 * @return
	 */
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


