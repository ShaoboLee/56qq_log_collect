package com.wlqq.bigdata.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestHdfsCompressStream {
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, URISyntaxException{
		Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://v30:8020"),conf);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        //ָ��ѹ���ļ�·��
        FSDataOutputStream out = fs.create(new Path("hdfs://v30:8020/storm/10001/test1.gz"));
        
        CompressionOutputStream compressOut = codec.createOutputStream(out);  
        
        File file = new File("data/10001-storm-to-hdfs-9-9-1457421433582");
        BufferedReader reader = null;
        try {
            System.out.println("����Ϊ��λ��ȡ�ļ����ݣ�һ�ζ�һ���У�");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // һ�ζ���һ�У�ֱ������nullΪ�ļ�����
            int k=1;
            int batch = 50000;
            while ((tempString = reader.readLine()) != null) {
                // ��ʾ�к�
            	compressOut.write((tempString+"\n").getBytes());
            	if(k%batch==0){
            		//compressOut.flush();
            		out.hsync();
            		compressOut.close();
            		System.out.println(k);
            		System.exit(0);
            		   try {
            				Thread.currentThread().sleep(10000);
            			} catch (InterruptedException e) {
            				// TODO Auto-generated catch block
            				e.printStackTrace();
            			}
            	}
                k++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        compressOut.close();
        try {
			Thread.currentThread().sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
