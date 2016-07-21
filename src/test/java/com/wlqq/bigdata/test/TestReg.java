package com.wlqq.bigdata.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestReg {

	public static void main(String[] args){
		String str="{\"index\":{\"_id\":\"1\"}}\n{}\n";
		System.out.println(str);
		Pattern pattern = Pattern.compile("(\\d+)_[dh]$");
	 	String t= "a_10001_d";
    	Matcher matcher = pattern.matcher(t);
    	if(matcher.find()){
    		System.out.println(matcher.group(1));
    	}
    	//String taskId = matcher.group(0);
    	//System.out.println(taskId);
    		
    	Pattern pattern1 = Pattern.compile("href=/\"(.+?)/\"");
    	Matcher matcher1 = pattern1.matcher("<a href=/\"index.html/\">ึ๗าณ</a>");
    	if(matcher1.find()){
    	  System.out.println(matcher1.group(1));
    	}
	}
}
