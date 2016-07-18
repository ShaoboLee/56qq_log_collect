package com.wlqq.bigdata.storm.hdfs.bolt.rotation;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


import com.wlqq.bigdata.storm.hdfs.bolt.rotation.FileRotationPolicy;
import com.wlqq.bigdata.utils.Utils;

import backtype.storm.tuple.Tuple;

/**
 * �������ļ��Ĺ���
 * 1����������Ҫ������ʱ��
 * ����
 * 2���ļ��ﵽ��ָ�����ļ���С
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
	 * �ж��Ƿ���Ҫ�����µ��ļ�
	 */
	public boolean mark(Tuple tuple, long offset) {
		// TODO Auto-generated method stub
        
		String currentHour = df.format(new Date());
		int hour = Integer.parseInt(currentHour.substring(8, 10));
		int day = Integer.parseInt(currentHour.substring(0, 8));
		
		if(intervalHour!=24){//partition by hour
			if(partitionId==-1){//������¼
				partitionId = hour/intervalHour;
				return false;
			}
			
			//�ж��Ƿ񵽴������ʱ�䣬ǰ������partitionId��һ���������·����ļ�
			if(partitionId!=hour/intervalHour){
				partitionId = hour/intervalHour;
				return true;
			}
		}else{//by day
			if(partitionId==-1){//������¼
				partitionId = day;
				return false;
			}
			if(partitionId!=day){
				partitionId = day;
				return true;
			}
		}
		
		
		
		//����һ����С����������һ���ļ�
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
