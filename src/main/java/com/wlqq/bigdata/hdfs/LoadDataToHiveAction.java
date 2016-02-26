package com.wlqq.bigdata.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.wlqq.bigdata.common.Utils;
import com.wlqq.bigdata.jdbc.HiveJDBC;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 将hdfs里面生成的文件，load到hive里面
 * @author wangchi
 *
 */
public class LoadDataToHiveAction implements RotationAction {
    private static final Log LOG = LogFactory.getLog(LoadDataToHiveAction.class);

    private int intervalHour;
    private String tablename;
    private String database;
    private String dayPartitonName;
    private String hourPartitionName;

    SimpleDateFormat sdf;
    String day,hour;//生成文件的时间
    SimpleDateFormat dfHour=new SimpleDateFormat("HH");
    String hourPartition;//对应的小时分区
    HiveJDBC hiveJDBC;
    
	boolean init = false;
	boolean flag = true;//jdbc init;
	private String hivehost;
	private String hiveport;
	private String hiveJsonSerdeJarPath;
	private String hiveFileFormat;
    
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
    }

    /**
     * filePath 可以找到老文件对应开始时间
     */
    public void execute(FileSystem fileSystem, Path filePath) throws IOException {
    	
    	String sql;
    	String unixtime = filePath.getName().split("-")[filePath.getName().split("-").length-1].split("\\.")[0];
    	day = sdf.format(new Date(Long.parseLong(unixtime))); 
    	
    	if(intervalHour==24){
    		sql = "load data inpath '"+filePath.toString()+"' into table "+database+"."+tablename+" partition("+dayPartitonName+"="+"'"+day+"'"+")";
    	}else{
    		hour = dfHour.format(new Date(Long.parseLong(unixtime))); 
    		int partition = Integer.parseInt(hour)/intervalHour*intervalHour;
    		hourPartition = partition<10?"0"+partition:partition+"";
    		sql = "load data inpath '"+filePath.toString()+"' into table "+database+"."+tablename
    				+" partition("+dayPartitonName+"="+"'"+day+"'"+","+hourPartitionName+"="+"'"+hourPartition+"'"+")";
    	}
    	
    	LOG.info("excute sql--->"+sql);
    	
    	if(!init){//init one time
    		hiveJDBC = new HiveJDBC(hivehost, hiveport);
    	}
    	
    	if(hiveFileFormat.equalsIgnoreCase("json")){//add jar 
    		String addjar = "add jar "+hiveJsonSerdeJarPath;
    		hiveJDBC.loadData(addjar);
    	}
    	
    	hiveJDBC.loadData(sql);
    }
    
    public static void main(String[] args){
    	Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"D:\\workspace\\hcb\\storm-hdfs-1001.yaml":args[0], true);
    	LoadDataToHiveAction load = new LoadDataToHiveAction(1, userConfig);
    	String oldFileName = "/storm/1001/storm-to-hdfs-14-99-1454810411488.txt";
    	try {
			load.execute(null, new Path(oldFileName));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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