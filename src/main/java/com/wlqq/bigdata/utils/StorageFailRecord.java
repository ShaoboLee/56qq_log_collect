package com.wlqq.bigdata.utils;

public interface StorageFailRecord {
	
	public void storage(String record,String topic,Exception exception,String message);

}
