package individualShip;

import it.sauronsoftware.base64.Base64;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class MainClass {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//String mmsi="413623000";
		String mmsi="353111000";
		fullShipsToFile();
		//allShipsToFile();
		//shipsToFile();
		
		ArrayList<String> mmsis=new ArrayList<String>();
		mmsis=queryShips();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
	
		for (int i =0;i<mmsis.size();i++){
			mmsi=mmsis.get(i);
			
			String hql="select * from t41_ais_history where row=^'"+mmsi+"' and '2013-01-01' > TIMESTAMP > '2012-01-01'";
			
			AisDataProcess processor= new AisDataProcess();
			List<AisPoint> points=processor.processAisData(hql);
			//System.out.println(i+" "+points.size()+" "+ mmsi);
			if (points.size() > 100000&&points.size()<150000) {
				
				System.out.println(i+" "+points.size()+" "+ mmsi);

				FileWriter fw = new FileWriter("e:/outputs/container/" + mmsi
						+ ".txt");// 创建FileWriter对象，用来写入字符流
				BufferedWriter bw = new BufferedWriter(fw); // 将缓冲对文件的输出
				processor.outputAisDate(points, bw);

				bw.close();

			}

			
			//System.out.println("done");
					
		}
		
		
		
		
	}
	
	
	public static ArrayList<String> queryShips() {

		ResultSet rs = null;
		Statement st;
		ArrayList<String> mmsis= new ArrayList<String>();
		
		String querySql ="select shipid,mmsi,speed,powerkw,dwt,type_en from shipview where mmsi is not null and powerkw >0 and type_en is not null and type_en='container'";
		Connection conn = getConnection(); // 同样先要获取连接，即连接到数据库
		
		try {
			st = conn.createStatement(); // 创建用于执行静态sql语句的Statement对象，st属局部变量
			rs = st.executeQuery(querySql); // 执行sql查询语句，返回查询数据的结果集
			while (rs.next()) { // 判断是否还有下一个数据

			    mmsis.add(rs.getString("mmsi"));
				
			}
			
			conn.close();
		} catch (SQLException e) {
			System.out.println("查询数据失败" + e.getMessage());
		}
	
		return mmsis;

	}
	
	//save container ships to file
	public static void shipsToFile() throws IOException{
				
		ResultSet rs = null;
		Statement st;
				
		String querySql ="select shipid,mmsi,speed,powerkw,dwt,type_en from shipview where mmsi is not null and powerkw >0 and type_en is not null and type_en='container'";
		Connection conn = getConnection(); // 同样先要获取连接，即连接到数据库
		String line;
		String header="mmsi speed powerkw dwt";
		FileWriter fw = new FileWriter("e:/outputs/ships/containerShip.txt");// 创建FileWriter对象，用来写入字符流
		BufferedWriter bw = new BufferedWriter(fw); // 将缓冲对文件的输出
		
		try {
			st = conn.createStatement(); // 创建用于执行静态sql语句的Statement对象，st属局部变量
			rs = st.executeQuery(querySql); // 执行sql查询语句，返回查询数据的结果集
			bw.write(header);
			bw.newLine();
			while (rs.next()) { // 判断是否还有下一个数据

			    line=rs.getString("mmsi")+" "+rs.getString("speed")+" "+rs.getString("powerkw")+" "+rs.getString("dwt");
			    bw.write(line);
			    bw.newLine();
  
			}
			bw.close();
			
			conn.close();
		} catch (SQLException e) {
			System.out.println("查询数据失败" + e.getMessage());
		}
		
	}
	
	//save all ships to file
		public static void allShipsToFile() throws IOException{
					
			ResultSet rs = null;
			Statement st;
					
			String querySql ="select mmsi, imo, callsign, speed, powerkw, dwt, grosston, max_length, beam, draft, type_en from shipview where mmsi is not null and powerkw >0 and type_en is not null";
			Connection conn = getConnection(); // 同样先要获取连接，即连接到数据库
			String line;
			String header="mmsi,imo,callsign,speed,powerkw,dwt,grosston,mlength,beam,draft,type_en";
			FileWriter fw = new FileWriter("e:/outputs/ships/allShip.txt");// 创建FileWriter对象，用来写入字符流
			BufferedWriter bw = new BufferedWriter(fw); // 将缓冲对文件的输出
			
			try {
				st = conn.createStatement(); // 创建用于执行静态sql语句的Statement对象，st属局部变量
				rs = st.executeQuery(querySql); // 执行sql查询语句，返回查询数据的结果集
				bw.write(header);
				bw.newLine();
				while (rs.next()) { // 判断是否还有下一个数据

				    line=rs.getString("mmsi")+","+rs.getString("imo")+","+rs.getString("callsign")+","
				    +rs.getString("speed")+","+rs.getString("powerkw")+","+rs.getString("dwt")+","
				    +rs.getString("grosston")+","+rs.getString("max_length")+","+rs.getString("beam")+","
				    +rs.getString("draft")+","+rs.getString("type_en");
				    bw.write(line);
				    bw.newLine();
	  
				}
				bw.close();
				
				conn.close();
			} catch (SQLException e) {
				System.out.println("查询数据失败" + e.getMessage());
			}
			
		}
		
		
		public static void fullShipsToFile() throws IOException{
			
			ResultSet rs = null;
			Statement st;
					
			String querySql ="select mmsi, imo, callsign, speed, powerkw, dwt, grosston, max_length, beam, draft, type_en from ships";
			Connection conn = getConnection(); // 同样先要获取连接，即连接到数据库
			String line;
			String header="mmsi,imo,callsign,speed,powerkw,dwt,grosston,mlength,beam,draft,type_en";
			FileWriter fw = new FileWriter("e:/outputs/ships/ships.txt");// 创建FileWriter对象，用来写入字符流
			BufferedWriter bw = new BufferedWriter(fw); // 将缓冲对文件的输出
			
			try {
				st = conn.createStatement(); // 创建用于执行静态sql语句的Statement对象，st属局部变量
				rs = st.executeQuery(querySql); // 执行sql查询语句，返回查询数据的结果集
				bw.write(header);
				bw.newLine();
				while (rs.next()) { // 判断是否还有下一个数据

				    line=rs.getString("mmsi")+","+rs.getString("imo")+","+rs.getString("callsign")+","
				    +rs.getString("speed")+","+rs.getString("powerkw")+","+rs.getString("dwt")+","
				    +rs.getString("grosston")+","+rs.getString("max_length")+","+rs.getString("beam")+","
				    +rs.getString("draft")+","+rs.getString("type_en");
				    bw.write(line);
				    bw.newLine();
	  
				}
				bw.close();
				
				conn.close();
			} catch (SQLException e) {
				System.out.println("查询数据失败" + e.getMessage());
			}
			
		}
	
	public static Connection getConnection() {

		Connection con = null; // 创建用于连接数据库的Connection对象
		try {
			Class.forName("com.mysql.jdbc.Driver");// 加载Mysql数据驱动

			con = DriverManager.getConnection(
					"jdbc:mysql://192.168.9.202:3306/boloomodb", "root",
					"wE32v1Zqy");// 创建数据连接

		} catch (Exception e) {
			System.out.println("数据库连接失败" + e.getMessage());
		}

		return con; // 返回所建立的数据库连接

	}

}
