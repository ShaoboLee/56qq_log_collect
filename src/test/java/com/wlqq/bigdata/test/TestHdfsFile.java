package com.wlqq.bigdata.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHdfsFile {
	
	public static void main(String[] args) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://v30:8020"),conf);
		FileStatus  status = fs.getFileStatus(new Path("/storm/10001/10001-storm-to-hdfs-8-4-1457764904516.txt"));
		System.out.println("accessTime="+status.getAccessTime());
		System.out.println("getModificationTime="+status.getModificationTime());
	}

}
