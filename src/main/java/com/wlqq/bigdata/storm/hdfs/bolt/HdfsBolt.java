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
package com.wlqq.bigdata.storm.hdfs.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wlqq.bigdata.storm.hdfs.bolt.format.FileNameFormat;
import com.wlqq.bigdata.storm.hdfs.bolt.format.RecordFormat;
import com.wlqq.bigdata.storm.hdfs.bolt.rotation.FileRotationPolicy;
import com.wlqq.bigdata.storm.hdfs.bolt.sync.SyncPolicy;
import com.wlqq.bigdata.storm.hdfs.common.rotation.RotationAction;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class HdfsBolt extends AbstractHdfsBolt{
	
	private static final long serialVersionUID = 1208459715177622190L;
    private static final Logger LOG = LoggerFactory.getLogger(HdfsBolt.class);

    private transient FSDataOutputStream out;
    private RecordFormat format;
    private long offset = 0;
    int interval = 1;

    public HdfsBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public HdfsBolt withInterval(int interval){
        this.interval = interval;
        return this;
    }
    public HdfsBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public HdfsBolt withFileNameFormat(FileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public HdfsBolt withRecordFormat(RecordFormat format){
        this.format = format;
        return this;
    }

    public HdfsBolt withSyncPolicy(SyncPolicy syncPolicy){
        this.syncPolicy = syncPolicy;
        return this;
    }

    public HdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public HdfsBolt addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        Calendar calendar = Calendar.getInstance();  
        Timer timer = new Timer();
        Date date;
        if(interval==24){//day
        	calendar.add(Calendar.DAY_OF_YEAR,1);//第二天
        	calendar.set(Calendar.HOUR_OF_DAY, 0); //
            calendar.set(Calendar.MINUTE, 0);  
            calendar.set(Calendar.SECOND, 0); 
            date=calendar.getTime();
            timer.scheduleAtFixedRate(new MyTask(), date, 24*60*60*1000); 
        }else{//hour
        	int nextHour = calendar.getTime().getHours()/interval*interval+interval;
        	if(nextHour>=24){
        		calendar.add(Calendar.DAY_OF_YEAR,1);//第二天
            	calendar.set(Calendar.HOUR_OF_DAY, 0); 
                calendar.set(Calendar.MINUTE, 0);  
                calendar.set(Calendar.SECOND, 0); 
        	}else{
            	calendar.set(Calendar.HOUR_OF_DAY, nextHour);
                calendar.set(Calendar.MINUTE, 0);  
                calendar.set(Calendar.SECOND, 0); 
        	}
        	date=calendar.getTime();
        	timer.scheduleAtFixedRate(new MyTask(), date, interval*60*60*1000); 
        }
    }
    
    class MyTask extends TimerTask {

		@Override
		public void run() {
			
			String filePath = currentFile.getName();
			String unixtime = filePath.split("-")[filePath.split("-").length-1].split("\\.")[0];
			
//			if(offset==0){//没有写入过数据
//				return;
//			}
			
			if(interval==24){
				SimpleDateFormat day_sdf = new SimpleDateFormat("yyyy-MM-dd");
				String day = day_sdf.format(new Date(Long.parseLong(unixtime))); 
				String currentDay = day_sdf.format(new Date(System.currentTimeMillis())); 
				if(day.equals(currentDay)){
					return;
				}
			}else{
				SimpleDateFormat hour_sdf = new SimpleDateFormat("yyyy-MM-dd HH"); 
				String hour = hour_sdf.format(new Date(Long.parseLong(unixtime))); 
				String currentHour = hour_sdf.format(new Date(System.currentTimeMillis())); 
				if(currentHour.equals(hour)){
					return;
				}
			}
			
			try {
				LOG.info("Timer start rotation file:"+currentFile.toString());
				rotateOutputFile();
				offset = 0;
	            rotationPolicy.reset();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
            
		}
    	
    }

    public void execute(Tuple tuple) {
        try {
        	
            byte[] bytes = this.format.format(tuple);
            synchronized (this.writeLock) {
                out.write(bytes);
                this.offset += bytes.length;
                
                if (this.syncPolicy.mark(tuple, this.offset)) {
                    if (this.out instanceof HdfsDataOutputStream) {
                        ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
                        //LOG.info("HdfsDataOutputStream");
                    } else {
                        this.out.hsync();
                        //LOG.info("FSDataOutputStream");
                    }
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
        this.out.close();
    }

    @Override
    Path createOutputFile() throws IOException {
        Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
        this.out = this.fs.create(path);
        return path;
    }
    public static void main(String[] args){
    	 SimpleDateFormat sdf;
    	 sdf = new SimpleDateFormat("yyyy-MM-dd");
    	 String filePath = "10001-storm-to-hdfs-8-17-1458526995929.txt";
         Calendar calendar = Calendar.getInstance();  
         calendar.set(Calendar.HOUR_OF_DAY, 1); //凌晨1点  
         calendar.set(Calendar.MINUTE, 0);  
         calendar.set(Calendar.SECOND, 0);  
         Date date=calendar.getTime(); //
         System.out.println(date.getTime());
			String unixtime = filePath.split("-")[filePath.split("-").length-1].split("\\.")[0];
			String day = sdf.format(new Date(Long.parseLong(unixtime))); 
			String currentDay = sdf.format(new Date(System.currentTimeMillis())); 
			if(day.equals(currentDay)){
				System.out.println("same day");
			}else{
				System.out.println("new day");
			}
			
         
    }
}
