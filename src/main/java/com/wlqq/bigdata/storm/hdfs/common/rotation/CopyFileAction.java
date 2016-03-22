package com.wlqq.bigdata.storm.hdfs.common.rotation;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlqq.bigdata.storm.hdfs.common.rotation.RotationAction;

import clojure.lang.Compiler.C;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 将hdfs里面生成的文件移动到指定的目录下面
 * @author wangchi
 *
 */
public class CopyFileAction implements RotationAction {
    private static final Logger LOG = LoggerFactory.getLogger(CopyFileAction.class);

    private String destination;
    private int intervalHour;
    private String tablePath;
    private String dayPartitonName;
    private String hourPartitionName;
    String id;
    Path destPath;
    SimpleDateFormat sdf;
    String day,hour;//生成文件的时间
    SimpleDateFormat dfHour=new SimpleDateFormat("HH");
    String hourPartition;//对应的小时分区
    
    public CopyFileAction(int intervalHour,String tablePath,String dayPartitonName,String hourPartitionName,String id,String dayFormat){
    	
    	this.intervalHour = intervalHour;
    	this.tablePath = tablePath;
    	this.dayPartitonName = dayPartitonName;
    	this.hourPartitionName = hourPartitionName;
    	this.id = id;
    	sdf=new SimpleDateFormat(dayFormat); 
    }

    public CopyFileAction toDestination(String destDir){
        destination = destDir;
        return this;
    }

    /**
     * filePath 可以找到老文件对应开始时间
     */
    public void execute(FileSystem fileSystem, Path filePath) throws IOException {
    	
    	String unixtime = filePath.getName().split("-")[filePath.getName().split("-").length-1].split("\\.")[0];
    	day = sdf.format(new Date(Long.parseLong(unixtime))); 
    	
    	if(intervalHour==24){
    		destination = tablePath+"/"+dayPartitonName+"="+day;
    		destPath = new Path(destination, id+"-"+unixtime+".log");
    	}else{
    		hour = dfHour.format(new Date(Long.parseLong(unixtime))); 
    		int partition = Integer.parseInt(hour)/intervalHour*intervalHour;
    		hourPartition = partition<10?"0"+partition:partition+"";
    		destination = tablePath+"/"+dayPartitonName+"="+day+"/"+hourPartitionName+"="+hourPartition;
    		destPath = new Path(destination, id+"-"+unixtime+".log");
    	}
        LOG.info("Moving file {} to {}", filePath, destPath);
        InputStream ins = fileSystem.open(filePath);
        OutputStream os = fileSystem.create(destPath);
        try{
            IOUtils.copy(ins, os);
            fileSystem.delete(filePath,true);
        }catch(Exception e){
            throw new IOException(e);
        }finally{
            ins.close();
            os.close();

        }

        return;
    }
    
    public static void main(String[] args){
    	String oldFileName = "/storm/1001/storm-to-hdfs-15-6-1454333721781";
    	String unixtime = oldFileName.split("-")[oldFileName.split("-").length-1].split("\\.")[0];
    	System.out.println(Long.parseLong(oldFileName.split("-")[oldFileName.split("-").length-1].split("\\.")[0]));
    	SimpleDateFormat sdf=new SimpleDateFormat("HH");  
    	  
    	String sd = sdf.format(new Date(Long.parseLong(unixtime)));  
    	System.out.println(sd);
    	int a = Integer.parseInt("04");
    	System.out.println(a);
    	int intervalHour = 3;
    	String hour = "08";
    	System.out.println(Integer.parseInt(hour)/intervalHour*intervalHour);
    	
    	
    }
}