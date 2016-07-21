/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wlqq.bigdata.test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlqq.bigdata.storm.hdfs.bolt.format.FileNameFormat;
import com.wlqq.bigdata.storm.hdfs.bolt.format.RecordFormat;
import com.wlqq.bigdata.storm.hdfs.bolt.rotation.FileRotationPolicy;
import com.wlqq.bigdata.storm.hdfs.bolt.sync.SyncPolicy;
import com.wlqq.bigdata.storm.hdfs.common.rotation.RotationAction;
import com.wlqq.bigdata.utils.Utils;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class HdfsBolt1 extends AbstractHdfsBolt1{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1208459715177622190L;

	private static final Logger LOG = LoggerFactory.getLogger(HdfsBolt1.class);

    private transient FSDataOutputStream out;
    private RecordFormat format;
    private long offset = 0;
    private CompressionCodec codec;
    private CompressionOutputStream compressOut; 
    private boolean compress;
    private boolean flag;

    public HdfsBolt1 withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public HdfsBolt1 withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public HdfsBolt1 withFileNameFormat(FileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public HdfsBolt1 withRecordFormat(RecordFormat format){
        this.format = format;
        return this;
    }

    public HdfsBolt1 withSyncPolicy(SyncPolicy syncPolicy){
        this.syncPolicy = syncPolicy;
        return this;
    }

    public HdfsBolt1 withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public HdfsBolt1 addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        String fileCompressClass = hdfsConfig.get(Utils.HIVE_FILE_COMPRESS_CLASS);
        LOG.info("compress class:"+fileCompressClass);
        
        compress = false;
        flag = true;
        if(fileCompressClass!=null && !"".equals(fileCompressClass)){//need compress
        	this.compress = true;
        	try {
    			Class<?> codecClass = Class.forName(fileCompressClass);
    			codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, hdfsConfig);
    			LOG.info("create codec success,flag="+this.flag);
    		} catch (ClassNotFoundException e) {
    			LOG.error("init class: "+fileCompressClass+" fail",e);
    			this.flag = false;//不往下执行了
    		}
        }else{
        	LOG.info("not compress");
        }
    	
    		
    }

    public void execute(Tuple tuple) {
    	//LOG.info("execute="+this.flag);
    	if(!this.flag){
    		LOG.error("compress config is invalid");
    		this.collector.fail(tuple);
    		return;
    	}
        try {
            byte[] bytes = this.format.format(tuple);
            synchronized (this.writeLock) {
            	if(this.compress){
            		compressOut.write(bytes);
            	}else{
            		out.write(bytes);
            	}
            	
                this.offset += bytes.length;

                if (this.syncPolicy.mark(tuple, this.offset)) {
                    this.out.hsync();
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

    @Override
    void closeOutputFile() throws IOException {
    	
    	this.compressOut.close();
//    	if(this.compress){
//    		this.compressOut.close();
//    	}else{
//    		this.out.close();
//    	}
    }

    @Override
    Path createOutputFile() throws IOException {
        Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
        this.out = this.fs.create(path);
        if(this.compress){
        	this.compressOut = codec.createOutputStream(out); 
        	LOG.info("create compressOut");
        }
        LOG.info("createOutputFile execute="+flag);
        return path;
    }
    
}

