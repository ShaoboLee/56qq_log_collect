package com.wlqq.bigdata.storm.hdfs.common.rotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.wlqq.bigdata.common.HdfsUtils;
import com.wlqq.bigdata.common.Utils;
import com.wlqq.bigdata.jdbc.HiveJDBC;
import com.wlqq.bigdata.storm.hdfs.common.rotation.RotationAction;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 将hdfs里面生成的文件，load到hive里面
 * @author wangchi
 *
 */
public class LoadDataToHiveAction implements RotationAction {
    /**
	 * 
	 */
	private static final long serialVersionUID = -6983114232761266570L;

	private static final Log LOG = LogFactory.getLog(LoadDataToHiveAction.class);

    private int intervalHour;
    private String tablename;
    private String database;
    private String dayPartitonName;
    private String hourPartitionName;

    private SimpleDateFormat sdf;
    private String day,hour;//生成文件的时间
    private SimpleDateFormat dfHour=new SimpleDateFormat("HH");
    private String hourPartition;//对应的小时分区
    private HiveJDBC hiveJDBC;
    
	private String hivehost;
	private String hiveport;
	private String hiveJsonSerdeJarPath;
	private String hiveFileFormat;
	private String hdfsurl;
	private String fileCompressClass;
	private String suffixName;
    
    public LoadDataToHiveAction(int intervalHour,Map<String, Object> userConfig){
    	this.intervalHour = intervalHour;
    	tablename = Utils.getValue(userConfig, Utils.HIVE_TABLE, "");
    	database = Utils.getValue(userConfig, Utils.HIVE_DATABASE, "test");
    	dayPartitonName = Utils.getValue(userConfig, Utils.HIVE_PARTITION_DAY_NAME, "_day_");
    	hourPartitionName = Utils.getValue(userConfig, Utils.HIVE_PARTITION_HOUR_NAME, "_hour_");
    	hivehost = Utils.getValue(userConfig, Utils.HIVE_HOST, "v29");
    	hiveport = Utils.getValue(userConfig, Utils.HIVE_PORT, "10000");
    	hiveJsonSerdeJarPath = Utils.getValue(userConfig, Utils.HIVE_JSON_SERDE_JAR_PATH, "");
    	hiveFileFormat = Utils.getValue(userConfig, Utils.HIVE_FILE_FORMAT, "");
    	sdf = new SimpleDateFormat(Utils.getValue(userConfig, Utils.HIVE_PARTITION_DAY_FORMAT, "yyyy-MM-dd")); 
    	hdfsurl = Utils.getValue(userConfig, Utils.HDFS_URL,"hdfs://localhost:9000");
    	suffixName = Utils.getValue(userConfig, Utils.HIVE_FILE_SUFFIX_NAME,"unknown");
    	fileCompressClass = Utils.getValue(userConfig, Utils.HIVE_FILE_COMPRESS_CLASS, "");
    }

    /**
     * filePath 可以找到老文件对应开始时间
     */
    public void execute(FileSystem fileSystem, Path filePath) {
    	
    	Time time = new SystemTime();
    	long begin = time.milliseconds();
    	
    	hiveJDBC = new HiveJDBC();
    	boolean flag = hiveJDBC.init(hivehost, hiveport);//init
    	
    	if(!flag){//hiveserver挂掉，立即返回
    		LOG.error("hive jdbc connect failed,filePath="+filePath.toString());
    		return;
    	}
    	
    	String oldPath = filePath.toString();
    	
    	if(!"".equals(fileCompressClass)){//deal compress
    		LOG.info("deal compress");
    		String newPath = "";
    		
    		try {
    			Class<?> codecClass = Class.forName(fileCompressClass);
    	        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, fileSystem.getConf());
    	        newPath = filePath.toString().replaceAll("\\.txt", codec.getDefaultExtension());
    			//newPath = oldPath.replaceAll("txt", suffixName);
    	        HdfsUtils.compress(fileSystem, codec, filePath, new Path(newPath));
				//compress(fileSystem,codec,filePath,newPath);
				if(load(newPath)){
					HdfsUtils.delete(fileSystem, oldPath);
					//delete(fileSystem,new Path(oldPath));//压缩文件load到hive成功后，再删除txt文件
				}else{
					load(oldPath);
					HdfsUtils.delete(fileSystem, newPath);
					//delete(fileSystem,new Path(newPath));
				}
			} catch (Exception e) {//not compress
				// TODO Auto-generated catch block
				e.printStackTrace();
				LOG.error("compress file fail...,then load txt file",e);
				load(oldPath);
				HdfsUtils.delete(fileSystem,newPath);
			}
    	}else{//not compress
    		LOG.info("load txt file");
    		load(oldPath);
    	}
    	long end = time.milliseconds();
    	LOG.info("cost "+(end-begin)+" ms.");
    }
    		
    public boolean load(String filePath) {
    	String sql;
    	String unixtime = filePath.split("-")[filePath.split("-").length-1].split("\\.")[0];
    	day = sdf.format(new Date(Long.parseLong(unixtime))); 
    	
    	if(intervalHour==24){
    		sql = "load data inpath '"+filePath+"' into table "+database+"."+tablename+" partition("+dayPartitonName+"="+"'"+day+"'"+")";
    	}else{
    		hour = dfHour.format(new Date(Long.parseLong(unixtime))); 
    		int partition = Integer.parseInt(hour)/intervalHour*intervalHour;
    		hourPartition = partition<10?"0"+partition:partition+"";
    		sql = "load data inpath '"+filePath+"' into table "+database+"."+tablename
    				+" partition("+dayPartitonName+"="+"'"+day+"'"+","+hourPartitionName+"="+"'"+hourPartition+"'"+")";
    	}
    	
    	LOG.info("excute sql--->"+sql);
    	
    	//hiveJDBC = new HiveJDBC();
    	//hiveJDBC.init(hivehost, hiveport);//init
    	
    	if(hiveFileFormat.equalsIgnoreCase("json")){//add jar 
    		String addjar = "add jar "+hiveJsonSerdeJarPath;
    		hiveJDBC.loadData(addjar);
    	}
    	
    	boolean success = hiveJDBC.loadData(sql);
    	hiveJDBC.close();
    	return success;
    	
    }
    
//    public void compress(FileSystem fs,CompressionCodec codec,Path filePath,String newPath) throws Exception{
//    	
//        //指定压缩文件路径
//        FSDataOutputStream outputStream = fs.create(new Path(hdfsurl+newPath));
//        //指定要被压缩的文件路径
//        FSDataInputStream in = fs.open(filePath);
//        //创建压缩输出流
//        CompressionOutputStream out = codec.createOutputStream(outputStream);  
//        IOUtils.copyBytes(in, out, fs.getConf()); 
//        IOUtils.closeStream(in);
//        IOUtils.closeStream(out);
//    }
//    
//    public void delete(FileSystem fs,Path path){
//    	
//		try {
//			FileStatus  status = fs.getFileStatus(path);
//			if (status.isFile()) { //if exists, delete
//	            boolean isDel = fs.delete(path,false);
//	            if(isDel){
//	            	LOG.info("delete file "+path.toString()+"  success...");
//	            }else{
//	            	LOG.info("delete file "+path.toString()+"  fail...");
//	            }
//	            
//	        } else {
//	            LOG.error("file:"+path.toString()+" not exists");
//	        }
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			LOG.error("delete file "+path.toString()+"  fail...",e);
//		}
//        
//    }
    
    public static void main(String[] args){
    	Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/storm-hdfs-10001.yaml":args[0], true);
    	
    	String suffixName = Utils.getValue(userConfig, Utils.HIVE_FILE_SUFFIX_NAME,"txt");
    	String fileCompressClass = Utils.getValue(userConfig, Utils.HIVE_FILE_COMPRESS_CLASS, "");
    	
    	//LoadDataToHiveAction load = new LoadDataToHiveAction(24, userConfig);
    	String oldFileName = "/storm/10001/10001-storm-to-hdfs-8-23-1457013241531.txt";
    	String newPath = oldFileName.replaceAll("txt", suffixName);
    	System.out.println(newPath);
    	
//    	load.hdfsurl = "hdfs://v30:8020";
//    	load.fileCompressClass = "org.apache.hadoop.io.compress.GzipCodec";
//    	load.execute(null, new Path(oldFileName));
    	
    	
//    	String unixtime = oldFileName.split("-")[oldFileName.split("-").length-1].split("\\.")[0];
//    	System.out.println(Long.parseLong(oldFileName.split("-")[oldFileName.split("-").length-1].split("\\.")[0]));
//    	SimpleDateFormat sdf=new SimpleDateFormat("HH");  
//    	  
//    	String sd = sdf.format(new Date(Long.parseLong(unixtime)));  
//    	System.out.println(sd);
//    	int a = Integer.parseInt("04");
//    	System.out.println(a);
//    	int intervalHour = 3;
//    	String hour = "08";
//    	System.out.println(Integer.parseInt(hour)/intervalHour*intervalHour);
    	
    	
    }
}