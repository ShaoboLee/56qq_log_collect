package com.wlqq.bigdata.storm.hdfs.bolt.rotation;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


import com.wlqq.bigdata.storm.hdfs.bolt.rotation.FileRotationPolicy;
import com.wlqq.bigdata.utils.Utils;

import backtype.storm.tuple.Tuple;

/**
 * 生成新文件的规则：
 * 1）到达了需要分区的时间
 * 或者
 * 2）文件达到了指定的文件大小
 * 
 * @author wangchi
 *
 */
public class PartitionByHourPolicy implements FileRotationPolicy{
	
	int intervalHour;
	int partitionId;
	SimpleDateFormat df; 
	
    public static enum Units {

        KB((long)Math.pow(2, 10)),
        MB((long)Math.pow(2, 20)),
        GB((long)Math.pow(2, 30)),
        TB((long)Math.pow(2, 40));

        private long byteCount;

        private Units(long byteCount){
            this.byteCount = byteCount;
        }

        public long getByteCount(){
            return byteCount;
        }
    }

    private long maxBytes;

    private long lastOffset = 0;
    private long currentBytesWritten = 0;

	private static final long serialVersionUID = 1L;
	
	public PartitionByHourPolicy(float count, Units units,int intervalHour){
		
		this.maxBytes = (long)(count * units.getByteCount());
		this.intervalHour = intervalHour;
		partitionId = -1;
		df = new SimpleDateFormat("yyyyMMddHH");
	}

	/**
	 * 判断是否需要生成新的文件
	 */
	public boolean mark(Tuple tuple, long offset) {
		// TODO Auto-generated method stub
        
		String currentHour = df.format(new Date());
		int hour = Integer.parseInt(currentHour.substring(8, 10));
		int day = Integer.parseInt(currentHour.substring(0, 8));
		
		if(intervalHour!=24){//partition by hour
			if(partitionId==-1){//首条记录
				partitionId = hour/intervalHour;
				return false;
			}
			
			//判断是否到达分区的时间，前后两个partitionId不一样就生成新分区文件
			if(partitionId!=hour/intervalHour){
				partitionId = hour/intervalHour;
				return true;
			}
		}else{//by day
			if(partitionId==-1){//首条记录
				partitionId = day;
				return false;
			}
			if(partitionId!=day){
				partitionId = day;
				return true;
			}
		}
		
		
		
		//到达一定大小就重新生成一个文件
        long diff = offset - this.lastOffset;
        this.currentBytesWritten += diff;
        this.lastOffset = offset;
        
        if(this.currentBytesWritten >= this.maxBytes){
        	return true;
        }
        
        return false;
        
	}

	public void reset() {//update path
		// TODO Auto-generated method stub
		
        this.currentBytesWritten = 0;
        this.lastOffset = 0;
		
	}

}
