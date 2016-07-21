package com.wlqq.bigdata.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

public class CompressTest {
    //压缩文件
    public static void compress(String codecClassName) throws Exception{
       //Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
//        conf.addResource("conf/capacity-scheduler.xml");
//        conf.addResource("conf/hadoop-policy.xml");
//        conf.addResource("conf/mapred-site.xml");
//        conf.addResource("conf/ssl-client.xml");
//        conf.addResource("conf/ssl-server.xml");
//        conf.addResource("conf/yarn-site.xml");
//        conf.addResource("conf/hdfs-site.xml");
//        System.out.println(conf.get("dfs.nameservices"));
        FileSystem fs = FileSystem.get(new URI("hdfs://v30:8020"),conf);
        
        //fs.getConf().set("LD_LIBRARY_PATH", "/usr/hdp/2.3.2.0-2950/hadoop/lib/native/");
        
//        List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
//		for (Class<? extends CompressionCodec> cls : codecs) {
//			System.out.println(cls.getName());
////			if (codecMatches(cls, codecName)) {
////				return ReflectionUtils.newInstance(cls, conf);
////			}
//		}
       // System.out.println(fs.getConf());
		CompressionCodec codec = getCodecByName(codecClassName,new Configuration());
		System.out.println(codec.getDefaultExtension());
		if(codec==null){
			System.out.println("codec is null");
		}else{
			System.out.println("codec is not null");
		}
		
//		System.load(file1);
//		System.load(file2);
//		
//		System.loadLibrary("libhadoop");
//		System.loadLibrary("libsnappy");
        
        //CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        //指定压缩文件路径
        FSDataOutputStream outputStream = fs.create(new Path("hdfs://v30:8020/storm/10001/10001-storm-to-hdfs-8-6-1457754933207.snappy"));
        //指定要被压缩的文件路径
        FSDataInputStream in = fs.open(new Path("hdfs://v30:8020/storm/10001/10001-storm-to-hdfs-8-6-1457754933207.txt"));
        //创建压缩输出流
        CompressionOutputStream out = codec.createOutputStream(outputStream);  
        IOUtils.copyBytes(in, out, conf); 
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }
    
    private static CompressionCodec getCodecByName(String codecName, Configuration conf) {
		List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
		for (Class<? extends CompressionCodec> cls : codecs) {
			System.out.println(cls.getName());
			if (codecMatches(cls, codecName)) {
				return ReflectionUtils.newInstance(cls, conf);
			}
		}
		return null;
	}
    
    private static boolean codecMatches(Class<? extends CompressionCodec> cls,
    	      String codecName) {
    	    String simpleName = cls.getSimpleName();
    	    if (cls.getName().equals(codecName)
    	        || simpleName.equalsIgnoreCase(codecName)) {
    	      return true;
    	    }
    	    if (simpleName.endsWith("Codec")) {
    	      String prefix = simpleName.substring(0, simpleName.length()
    	          - "Codec".length());
    	      if (prefix.equalsIgnoreCase(codecName)) {
    	        return true;
    	      }
    	    }
    	    return false;
    	  }
    
    //解压缩
    public static void uncompress(String fileName) throws Exception{
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        FSDataInputStream inputStream = fs.open(new Path("/user/hadoop/text.gz"));
         //把text文件里到数据解压，然后输出到控制台  
        InputStream in = codec.createInputStream(inputStream);  
        IOUtils.copyBytes(in, System.out, conf);
        IOUtils.closeStream(in);
    }
    
    //使用文件扩展名来推断二来的codec来对文件进行解压缩
    public static void uncompress1(String uri) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        
        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if(codec == null){
            System.out.println("no codec found for " + uri);
            System.exit(1);
        }
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally{
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }
    
    
    
    public static void main(String[] args) throws Exception {
    	System.out.println("***** "+System.getProperty("java.library.path"));
//    	Time time = new SystemTime();;
//    	long begin = time.milliseconds();
        compress("org.apache.hadoop.io.compress.SnappyCodec");
//    	Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(new URI("hdfs://v30:8020"),conf);
//        conf=fs.getConf();
//        System.out.println(conf.get("LD_LIBRARY_PATH"));
//        System.out.println(CompressionCodecFactory.getCodecClasses(conf));
//        boolean nativeHadoopLoaded = NativeCodeLoader.isNativeCodeLoaded();
//        System.out.println(nativeHadoopLoaded);
//        SnappyCodec.isNativeCodeLoaded();
//        NativeCodeLoader nc;
//        long end = time.milliseconds();
//        System.out.println((end-begin)+" ms.");
        //uncompress("text");
        //uncompress1("hdfs://master:9000/user/hadoop/text.gz");
//    	String currentHour = "2016031009";
//    	int day = Integer.parseInt(currentHour.substring(0, 8));
//    	System.out.println(day);
    }

}
