package com.wlqq.bigdata.jdbc;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
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
  
	/**
	 * 获取表对应的分区名称
	 * @param database
	 * @param sql
	 * @return
	 */
	public String getPartition(String database,String table,String hiveJsonSerdeJarPath){
		
		ResultSet rs;
		String p = null;
		String sql = "desc "+table;
		try {
		    String addjar = "add jar "+hiveJsonSerdeJarPath;
		    stmt.execute(addjar);
			stmt.execute("use "+database);
			rs = stmt.executeQuery(sql);
			boolean find = false;
			while(rs.next()){
				String tmp = rs.getString(1);
				if(tmp.equals("# Partition Information")){
					find = true;
				}
				if(find){
					if(tmp==null || tmp.length()==0 || tmp.startsWith("#")){
						continue;
					}else{
						p = p==null?tmp:(p+","+tmp);
					}
				}
			}
			return p;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("hivesql execute failed"+"\n"+sql);
			return p;
		}
	}
  
 
	public List<String> showTables(String database){
		
		ResultSet rs;
		String sql = "show tables";
		List<String> list = new ArrayList<String>();
		try {
			stmt.execute("use "+database);
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				list.add(rs.getString(1));
			}
			return list;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("hivesql execute failed"+"\n"+sql);
			return list;
		}
	}
  
	public static void main(String[] args) throws SQLException {
		HiveJDBC jdbc = new HiveJDBC();
		jdbc.init("v29", "10000");
		String table = "operation_activity_10004_d";
		//jdbc.loadData("add jar hdfs://c1/apps/hive/udfjars/json-serde-1.3.8-SNAPSHOT-jar-with-dependencies.jar");
		//jdbc.loadData("use client_business_log");
		System.out.println(jdbc.getPartition("client_business_log",table,"hdfs://c1/apps/hive/udfjars/json-serde-1.3.8-SNAPSHOT-jar-with-dependencies.jar"));
		String database = "client_business_log";
		System.out.println(jdbc.showTables(database));
		//t1002_d
//		jdbc.loadData("add jar hdfs://c1/apps/hive/udfjars/json-serde-1.3.8-SNAPSHOT-jar-with-dependencies.jar");
//		jdbc.loadData("load data inpath '/storm/10001/10001-storm-to-hdfs-8-12-1457587388180.txt' into table test.t10001_d partition(dt='2016-03-10')");
//		jdbc.loadData("load data inpath '/storm/1001/1001-storm-to-hdfs-9-1-1456471921934.txt' into table test.t1001_d_h partition(day='2016-02-26',hour='12')");
//		jdbc.search("select * from test.t1002_d;");
		//jdbc.loadData("list jar");
		//jdbc.loadData("list jar;");
  }
}
