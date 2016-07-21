package com.wlqq.bigdata.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import com.wlqq.bigdata.utils.HdfsUtils;

public class TestHdfsStream {
	
	public static long readLastLine( FSDataInputStream fsInput,long len) throws IOException { 
//		byte[] b2 = new byte[1024];
//		fsInput.read(b2);
//		System.out.println(new String(b2));
		fsInput.seek(len+1);  
		      long pos = len - 1;  
		      while (pos > 0) {
		        pos--;  
		        fsInput.seek(pos);  
		        if (fsInput.readByte() == '\n') {  
		          break;  
		        }  
		      }  
		      if (pos == 0) {  
		    	  fsInput.seek(0);  
		      }  
		      
		      System.out.println(fsInput.readLine());
		      fsInput.seek(pos);  
		      byte[] bytes = new byte[(int) (len - pos)];  
		      System.out.println("arr len="+bytes.length);
		      fsInput.read(bytes);  
		      System.out.println("pos="+pos);
		      System.out.println(new String(bytes)+"**");
		      System.out.println(new String(bytes).length());
		      fsInput.seek(len-2);
		      byte[] b1 = new byte[1];
		      //fsInput.read(b1);
		      System.out.println(fsInput.readByte()=='}');
		      return pos;
		}  
	
	public static void copyStream(FSDataInputStream in,FSDataOutputStream out,FileSystem fs) throws IOException, InterruptedException{
		IOUtils.copyBytes(in, out, fs.getConf());
		System.out.println("sleep 120s");
		Thread.currentThread().sleep(10000000);
		IOUtils.closeStream(in);
		IOUtils.closeStream(out);
	}
	
	public static void main(String[] args) throws IllegalArgumentException, Exception{
		Time time = new SystemTime();
    	long begin = time.milliseconds();
		FSDataOutputStream outputStream;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://v30:8020"),conf);
		Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        Path path = new Path("/storm/10001/10001-storm-to-hdfs-8-17-1458526995929.txt");
        Path outPath = new Path("/storm/10001/10001-storm-to-hdfs-9-1-1458269249338.txt");
        //HdfsUtils.compress(fs, codec, path, outPath,true);
        FileStatus  status = fs.getFileStatus(path);
        
        long len = status.getLen();
        System.out.println("len="+len);
        FSDataInputStream fsInput = fs.open(path);
        System.out.println(HdfsUtils.getPos(fsInput, len));
		long end = time.milliseconds();
		System.out.println("cost "+(end-begin)+" ms.");
//        outputStream = fs.create(outPath);
//        copyStream(fsInput, outputStream, fs);
        
        //System.out.println(HdfsUtils.getPos(fsInput, len));
//        //268435273
//s        long pos = readLastLine(fsInput,len);
//        fsInput.seek(0);
//        byte buf[] = new byte[4096];
////        int bytesRead = fsInput.read(buf);
//        outputStream = fs.create(new Path("/storm1/10001/10001-storm-to-hdfs-9-1-1458269249338.txt_bak"));
//       // CompressionOutputStream out = codec.createOutputStream(outputStream);
//        long p = pos+1;
//        System.out.println("pos="+pos);
//        long num = p/4096;
//        int tail = (int) (p%4096);
//        int i = 1;
//        System.out.println("num="+num);
//        while (i<=num) {
//        	outputStream.write(buf, 0, 4096);
//            fsInput.read(buf);
//            i++;
//          }
//        byte t[] = new byte[tail];
//        fsInput.read(t);
//        outputStream.write(t, 0, tail);
//        long end = time.milliseconds();
//		System.out.println("cost "+(end-begin)+" ms.");
//		fsInput.seek(pos-1);
//		byte a = fsInput.readByte();
//		System.out.println(a);
//		System.out.println("}".getBytes()[0]);
//		outputStream.hsync();
//		outputStream.close();
        
//		HdfsUtils.compress(fs, codec, new Path("/storm1/10001/10001-storm-to-hdfs-9-1-1458269249338.txt"),
//				new Path("/storm1/10001/10001-storm-to-hdfs-9-1-1458269249338.gz"),true);
//		out = fs.create(new Path("/storm1/10001/10001-storm-to-hdfs-9-1-1458269249338.txt_bak"));
//		  BufferedReader reader = null;  
//		  int i = 1;
//		    try{  
//		        FSDataInputStream fsInput = fs.open(new Path("/storm1/10001/10001-storm-to-hdfs-9-1-1458269249338.txt"));  
//		        reader = new BufferedReader(new InputStreamReader(fsInput,"UTF-8"),256);  
//		        String line = null;  
//		        while (true) {  
//		            line = reader.readLine();  
//		            if(line == null) {  
//		                break;  
//		            }else{
//		            	line = line+"\n";
//		            }  
//		            
//		            out.write(line.getBytes());
//		            out.hsync();
//		            if(i%1000==0){
//		            	System.out.println("flush,i="+i);
//		            	long end = time.milliseconds();
//		            	System.out.println("cost "+(end-begin)+" ms.");
//		            	begin=end;
//		            }
//		            i++;
//		        }  
//		       // System.out.println("total line:"+i+",flush to hdfs line:"+j);
//		    }catch (Exception e) {  
//		        e.printStackTrace();  
//		    } finally {  
//		        IOUtils.closeStream(reader);  
//		    }  
//		
////		int i = 0;
////		while(i<10005){
////			String str = i+",hcb-ms\n";
////			out.write(str.getBytes());
////			if(i%1000==0){
////				//out.flush();
////				((HdfsDataOutputStream) out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
////			}
////			i++;
////		}
//		//Thread.currentThread().sleep(10000000);
//		out.close();
//		long end = time.milliseconds();
//		System.out.println("cost "+(end-begin)+" ms.");
	}

}
