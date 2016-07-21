package com.wlqq.bigdata.test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestArrayBlockingQueue {

	public static void main(String[] args) throws InterruptedException{
		
		ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(2000);
		int i = 0;
		while(i<3000){
			queue.offer(""+i, 1, TimeUnit.SECONDS);
			System.out.println(queue.size());
			i++;
		}
	}

}
