package com.wlqq.bigdata.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HAUtil;

public class GetHANamenode {
	public static void main(String[] args) throws Exception{

		System.out.println("begin..");
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"D:\\workspace\\hcb\\storm-hdfs-1001.yaml":args[0], true);
		Configuration conf = new Configuration();
        if(userConfig != null){
            for(String key : userConfig.keySet()){
            	System.out.println(key);
                conf.set(key, String.valueOf(userConfig.get(key)));
            }
        }
		FileSystem system=null;
		System.out.println(userConfig);
		try {
			system = FileSystem.get(URI.create("hdfs://v28:8020"), conf);
			InetSocketAddress active = HAUtil.getAddressOfActive(system);
			System.out.println("hdfs host:"+active.getHostName());  //hadoop001
			System.out.println("hdfs port:"+active.getPort());      // 9000
			InetAddress address = active.getAddress();
			System.out.println("hdfs://"+address.getHostAddress()+":"+active.getPort());  //   hdfs://192.168.8.21:9000
		} catch (IOException e) {
			throw new Exception(""+e.getMessage());
		}finally{
			try {
				if(system!=null){
					system.close();
				}
			}  catch (IOException e) {
				throw new Exception(""+e.getMessage());
			}
		}
	}

}
