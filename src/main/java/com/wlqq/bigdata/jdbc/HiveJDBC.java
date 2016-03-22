package com.wlqq.bigdata.jdbc;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
//import org.apache.hive.jdbc.HiveDriver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

 /**
  * load失败没有做处理，${hdfs.write.path}路径下面存放了不属于当前时刻的数据时，说明发生过load失败，可以在监控页面找到相应的失败信息
  * 
  * @author wangchi
  *
  */
public class HiveJDBC implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static final Log logger = LogFactory.getLog(HiveJDBC.class);
	private String driverName = "org.apache.hive.jdbc.HiveDriver";
	private Connection con;
	private Statement stmt; 
	//boolean flag = true;//jdbc init;
	
	public boolean init(String host,String port){
		try {
		      Class.forName(driverName);
		      con = DriverManager.getConnection("jdbc:hive2://"+host+":"+port+"/default", "", "");
		      stmt = con.createStatement();
		      return true;
		    } catch (ClassNotFoundException e) {
		      // TODO Auto-generated catch block
		      e.printStackTrace();
		      //flag = false;
		    } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//flag = false;
			}
		return false;
	}
	
	public boolean loadData(String sql){
		
//		if(!flag){//connect hive failed
//			logger.error("hive jdbc connect failed"+"\n"+sql);
//			return false;
//		}
		try {
			stmt.execute(sql);
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("hivesql execute failed"+"\n"+sql);
			return false;
		}
	}
	
	public void close(){
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
  
//	public void search(String sql){
//		
//		if(!flag){//connect hive failed
//			logger.error("hive jdbc connect failed"+"\n"+sql);
//		}
//		ResultSet rs;
//		try {
//			rs = stmt.executeQuery(sql);
//			if(rs.next()){
//				System.out.println(rs.getString(1));
//			}
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			logger.error("hivesql execute failed"+"\n"+sql);
//		}
//	}
  
 
  
	public static void main(String[] args) throws SQLException {
		HiveJDBC jdbc = new HiveJDBC();
		jdbc.init("v29", "10000");
		//t1002_d
		jdbc.loadData("add jar hdfs://c1/apps/hive/udfjars/json-serde-1.3.8-SNAPSHOT-jar-with-dependencies.jar");
		jdbc.loadData("load data inpath '/storm/10001/10001-storm-to-hdfs-8-12-1457587388180.txt' into table test.t10001_d partition(dt='2016-03-10')");
//		jdbc.loadData("load data inpath '/storm/1001/1001-storm-to-hdfs-9-1-1456471921934.txt' into table test.t1001_d_h partition(day='2016-02-26',hour='12')");
//		jdbc.search("select * from test.t1002_d;");
		//jdbc.loadData("list jar");
		//jdbc.loadData("list jar;");
  }
}
