package com.wlqq.bigdata.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class StorageToLocalDisk implements StorageFailRecord{
	
    private String path;
    private String newLine;
	
	public StorageToLocalDisk(String path){
		this.path = path;
		newLine = System.getProperty("line.separator");
	}
	
	public void storage(String record) {
		// TODO Auto-generated method stub
		
		String directory = path.substring(0,path.lastIndexOf(File.separator));
		File dir = new File(directory);
		
		if(!dir.exists()){
			dir.mkdirs();
		}
		
		File file=new File(path);
		FileOutputStream out;
		
        if(!file.exists()){
        	try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        try {
			out=new FileOutputStream(file,true);
			out.write((record+newLine).getBytes());
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args){
		StorageFailRecord sfr = new StorageToLocalDisk("d:\\a.txt");
		sfr.storage("hello world");
	}

}
