package com.wlqq.bigdata.common;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import com.alibaba.fastjson.JSONObject;

public class HdfsUtils {
	
	private static final Log logger = LogFactory.getLog(HdfsUtils.class);
	

	public static boolean delete(FileSystem fs,String path){
		return delete(fs,new Path(path));
	}
	
	public static boolean delete(FileSystem fs,Path path){
    	
		try {
			FileStatus  status = fs.getFileStatus(path);
			boolean isDel = false;
			if (status.isFile()) { //if exists, delete
	            isDel = fs.delete(path,false);
	            if(isDel){
	            	logger.info("delete file "+path.toString()+"  success...");
	            }else{
	            	logger.info("delete file "+path.toString()+"  fail...");
	            	
	            }
	            
	        } else {
	        	logger.error("file:"+path.toString()+" not exists");
	        }
			return isDel;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("delete file "+path.toString()+"  fail...",e);
			return false;
		}
        
    }
	
	public static void compress(FileSystem fs,CompressionCodec codec,Path filePath,Path newPath) throws Exception{
	    	
	        compress(fs,codec,filePath,newPath,false);
	 }
	
	/**
	 * 
	 * @param fs
	 * @param codec
	 * @param filePath
	 * @param newPath
	 * @param check 是否检查filePath对应文件的最后一行是否满足json格式
	 * @throws Exception
	 */
	public static void compress(FileSystem fs,CompressionCodec codec,Path filePath,Path newPath,boolean check) throws Exception{
		
		Time time = new SystemTime();
    	long begin = time.milliseconds();
		FSDataOutputStream outputStream = fs.create(newPath);
		FSDataInputStream in = fs.open(filePath);
		CompressionOutputStream out = codec.createOutputStream(outputStream);  
		if(!check){
			copyStream(in, out, fs);
		}else{
			FileStatus  status = fs.getFileStatus(filePath);
	        long len = status.getLen();
			long pos = getPos(in, len);
			if(pos==0){//最后一条记录是有效的json
				copyStream(in, out, fs);
			}else{
				byte buf[] = new byte[4096];
				long currentPos = 0;
				while(currentPos<pos){
					in.read(buf);
					out.write(buf, 0, (int) (pos-currentPos>=4096?4096:pos-currentPos));
					currentPos+=4096;
				}
//				long num = pos/4096;
//		        int tail = (int) (pos%4096);
//		        int i = 1;
//		        while (i<=num) {
//		        	out.write(buf, 0, 4096);
//		            in.read(buf);
//		            i++;
//		          }
//		        byte t[] = new byte[tail];
//		        in.read(t);//读取剩余部分
//		        out.write(t, 0, tail);
//		        outputStream.hsync();
		        //找到最后一条错误记录
		        in.seek(pos);
		        logger.error("invalid last one record : "+in.readLine());
		        in.close();
		        out.close();
			}
		}
		long end = time.milliseconds();
    	logger.info("compress file cost "+(end-begin)+" ms.");
	}
	
	public static void copyStream(FSDataInputStream in,CompressionOutputStream out,FileSystem fs) throws IOException{
		IOUtils.copyBytes(in, out, fs.getConf());
		IOUtils.closeStream(in);
		IOUtils.closeStream(out);
	}
	
	/**
	 * 
	 * @param in
	 * @param len FileSystem获取到的文件长度，有可能和实际的文件大小不一致（流没有关闭时，namenode没有实时更新文件的长度信息）
	 * 所以在处理时，需要考虑len和实际大小不一致的问题
	 * @return
	 * @throws IOException
	 */
	public static long getPos(FSDataInputStream in,long len) throws IOException {
		
		in.seek(len-2);
	    if(in.readByte()=='}'){//最后一条记录是有效的
	    	in.seek(0);
	    	return 0;
	    }else{
	    	long pos = len - 1;  
		    while (pos > 0) {
		    	pos--;  
		        in.seek(pos);  
		        if (in.readByte() == '\n') {  
		          break;  
		        }  
		    }  
		    in.seek(0);
		    return pos+1;//+1是为了添加\n
	    }
		
//		in.seek(len-1);
//		String line = in.readLine();
//		if(line==null){//文件大小和len一致
//			in.seek(len-2);
//		    if(in.readByte()=='}'){//最后一条记录是有效的
//		    	in.seek(0);
//		    	return 0;
//		    }else{
//		    	long pos = len - 1;  
//			    while (pos > 0) {
//			    	pos--;  
//			        in.seek(pos);  
//			        if (in.readByte() == '\n') {  
//			          break;  
//			        }  
//			    }  
//			    in.seek(0);
//			    return pos+1;//+1是为了添加\n
//		    }
//		}else{
//			long pos = len;
//			String tmp = null;
//			while((tmp = in.readLine())!=null){
//				pos += line.length();
//				System.out.println(line);
//				line = tmp;
//			}
//			if(line.endsWith("}")){
//				pos += line.length();
//			}
//			System.out.println(line);
//			System.out.println(pos);
//			pos += line.length();
//			in.seek(0);
//			return pos+1;
//		}
		
	}
	
	public static void main(String[] args){
		String line = "{}"+"\n";
		JSONObject a = JSONObject.parseObject(line); 
		System.out.println(a.toString());
	}

}
