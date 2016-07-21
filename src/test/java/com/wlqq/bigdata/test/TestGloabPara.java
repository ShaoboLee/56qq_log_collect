package com.wlqq.bigdata.test;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;

public class TestGloabPara {
	
	Map<String,Object> map;
	
	public TestGloabPara(){
		map = new HashMap<String,Object>();
	}
	
	class T1 {
		Map<String,Object> map;
		T1(Map<String,Object> map){
			this.map = map;
		}
	}

	class T2 {
		Map<String,Object> map;
		T2(Map<String,Object> map){
			this.map = map;
		}
	}
	
	public  static void main(String[] args){
		TestGloabPara tgp = new TestGloabPara();
		tgp.map.put("A", "a");
		tgp.map.put("Num", 1);
		
		TestGloabPara.T1 t1 = new TestGloabPara().new T1(tgp.map);
		TestGloabPara.T2 t2 = new TestGloabPara().new T2(tgp.map);
		t1.map.put("A", "b");
		System.out.println(t1.map);
		System.out.println(t2.map);
		System.out.println(tgp.map);
		
	}
}
