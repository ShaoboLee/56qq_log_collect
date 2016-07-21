package com.wlqq.bigdata.test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlqq.bigdata.storm.hdfs.bolt.HdfsBolt;
import com.wlqq.bigdata.storm.hdfs.bolt.format.RecordFormat;
import com.wlqq.bigdata.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class CompressHdfsBolt extends HdfsBolt{

	private static final Logger LOG = LoggerFactory.getLogger(CompressHdfsBolt.class);

    private transient FSDataOutputStream out;
    private CompressionOutputStream compressOut; 
    private long offset = 0;
    private Map<String, String> suffix; 
    private String suffixName;
    private CompressionCodec codec;
    private boolean compress = true;

    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        String fileCompressClass = hdfsConfig.get(Utils.HIVE_FILE_COMPRESS_CLASS);
        suffix = new HashMap<String,String>();
    	suffix.put("org.apache.hadoop.io.compress.GzipCodec", "gz");
    	suffix.put("org.apache.hadoop.io.compress.DefaultCodec", "deflate");
    	suffix.put("org.apache.hadoop.io.compress.DeflateCodec", "deflate");
    	suffix.put("org.apache.hadoop.io.compress.BZip2Codec", "bz2");
    	suffixName = suffix.get(fileCompressClass);
    	LOG.info("suffixName="+suffixName);
    	if(suffixName!=null){//compress
    		Class<?> codecClass = null;
    		try {
    			codecClass = Class.forName(fileCompressClass);
    			codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, hdfsConfig);
    			if(codec==null){
    				LOG.error("codec init fail");
    			}
                //compressOut = codec.createOutputStream(out); 
                compress = true;
    		} catch (ClassNotFoundException e) {
    			// TODO Auto-generated catch block
    			//e.printStackTrace();
    			LOG.error("create createOutputStream fail",e);
    		}
    	}else{
    		LOG.info("not compress");
    	}
        
    }

    public void execute(Tuple tuple) {
        try {
            //byte[] bytes = this.format.format(tuple);
        	Object o = tuple.getValueByField("record");
        	if(o==null){
        		LOG.error("tuple is null");
        		this.collector.ack(tuple);
        		return;
        	}
            byte[] bytes = o.toString().getBytes();
            
            synchronized (this.writeLock) {
            	
            	if(compress){
            		if(compressOut==null){
            			LOG.error("compressOut do not init");
            		}
            		compressOut.write(bytes);
            	}else{
            		out.write(bytes);
            	}
            	
                this.offset += bytes.length;

                if (this.syncPolicy.mark(tuple, this.offset)) {
                    this.compressOut.flush();
                    this.syncPolicy.reset();
                }
            }

            this.collector.ack(tuple);

            if(this.rotationPolicy.mark(tuple, this.offset)){
                rotateOutputFile(); // synchronized
                this.offset = 0;
                this.rotationPolicy.reset();
            }
        } catch (IOException e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    void closeOutputFile() throws IOException {
    	if(compress){
    		this.compressOut.close();
    	}else{
    		this.out.close();
    	}
    	
    }

    Path createOutputFile() throws IOException {
        Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
        this.out = this.fs.create(path);
        this.compressOut = codec.createOutputStream(out); 
        LOG.info("create compressOut");
        return path;
    }
}

