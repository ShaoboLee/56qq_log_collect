package com.wlqq.bigdata.es.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

public class TestFor {
	
	 static class A {
		String f1;
		String f2;
		public A(String f1,String f2){
			this.f1 = f1;
			this.f2 = f2;
		}
		public String getF1() {
			return f1;
		}
		public void setF1(String f1) {
			this.f1 = f1;
		}
		public String getF2() {
			return f2;
		}
		public void setF2(String f2) {
			this.f2 = f2;
		}
		
		public String toString(){
			return f1+"-"+f2;
		}
	}
	
	public static void main(String[] args){
		LinkedList<A> arr = new LinkedList<A>();
		HashSet<A> arrs = new HashSet<A>();
		arr.add(new A("1","1"));
		arr.add(new A("2","2"));
		arr.add(new A("3","3"));
		arr.add(new A("4","4"));
		arr.add(new A("5","5"));
		arr.add(new A("6","6"));
		for(A a:arr){
			System.out.println(a.toString());
		}
		//System.out.println(arr);
	}

}
