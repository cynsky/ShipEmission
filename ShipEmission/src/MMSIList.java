import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.HqlResult2;

/**
 * @author zhihuan
 *
 */

public class MMSIList {

	/**
	 * @param args
	 */
	
	static HqlResult2 aisRcd; // AIS records from hypertable
	static String querySql ="select shipid,mmsi,speed,powerkw,dwt,type_en from shipview "
			+ "where mmsi is not null and powerkw >0 and type_en is not null and type_en='container'";
	public static void main(String[] args) throws TTransportException, TException, ClientException {
		// TODO Auto-generated method stub
		
		List <Ship> ships =queryShips(querySql);
		Ship ship=ships.get(500);
		
		String hql = "select * from t41_ais_history where row=^" + "'"
				+ ship.getMMSI() + "'"
				+ "and '2013-01-01' > TIMESTAMP > '2012-01-01'";
		
		int count = 0;
		System.out.println("ship mmsi **********************: "+ship.getMMSI()+ "number of ships: " +ships.size());
		aisRcd=hqlQuery(hql);
		count=aisRcd.cells.size(); // the output number is set to be less then Integer.MAX_VALUE=2147483647
		System.out.println("count:*******"+count);
		

	}
	
	//get ais messages from hypertable
	
	public static HqlResult2 hqlQuery(String hql) throws TTransportException, TException,
	ClientException {
		
		ThriftClient client = ThriftClient.create("192.168.9.175", 38080);	
        long ns =client.open_namespace("/aisdb");  
        HqlResult2 ais;
        ais=client.hql_query2(ns, hql);
        client.close();
        return ais;
	}
	
	//extract a single ais messages 
	static List<String> extractAIS(List<String> record) {
		// 0,mmsi;1,timestamp;2,nav_status;3,rot;4,sog;5,pos_acc;6,lon;7,lat;
		// 8,cog;9,true_head;10,eta;11,dest_id;12,dest��13,src_id;14,dist_to_port;15,blm_avg_speed
		List<String> report = new ArrayList<String>();
		String[] key = record.get(0).split(" ");
		String[] value = record.get(3).split("@");
		// add key strings to report
		for (int j = 0; j < key.length; j++) {
			report.add(j, key[j]);
		}
		// add value strings to report
		for (int j = 0; j < value.length; j++) {
			report.add(key.length + j, value[j]);
		}
		return report;
	}

	//get ships from mysql 
	
	public static List<Ship> queryShips(String sql) {
		/**
		 * SELECT * FROM shipview WHERE mmsi IS NOT NULL;
           DROP VIEW shipview;
           CREATE VIEW shipview AS 
           SELECT ship.shipid shipid,ship.mmsi mmsi,ship.IMO imo,ship.CALLSIGN,ship.speed speed,eng.powerkw powerkw,ton.dwt dwt,tp.key1 typecode,tp.desc_en type_en,tp.desc_cn type_cn,
           ship.name shipname,ship.localname localname,ship.country_code country,ship.built built_date,ship.owner ship_owner,
           ton.grosston grosston, ton.netton netton,eng.builder builder,eng.number number,eng.rpm rpm,eng.stroke stroke,
           dim.loa max_length, dim.maxbeam beam, dim.draft draft
           FROM t41_ship ship INNER JOIN t41_ship_engine eng ON ship.shipid=eng.shipid INNER JOIN t41_ship_tonnage ton ON ship.shipid=ton.shipid 
           INNER JOIN t41_ship_dimension dim ON ship.shipid=dim.shipid INNER JOIN t91_code tp ON ship.shiptype_key=tp.key1 WHERE dwt>=100 AND speed>0;
		 */	
		ResultSet rs;
	    Connection conn;
		Statement st;
		List<Ship> records = new ArrayList<Ship>();
		conn = getConnection(); // 同样先要获取连接，即连接到数据库
		try {
			st = conn.createStatement(); // 创建用于执行静态sql语句的Statement对象，st属局部变量
			rs = st.executeQuery(sql); // 执行sql查询语句，返回查询数据的结果集
			while (rs.next()) { // 判断是否还有下一个数据
				// 根据字段名获取相应的值
			    Ship ship = new Ship();
				ship.setMMSI(rs.getString("mmsi"));
				ship.setDesignSpeed(Double.parseDouble(rs.getString("speed")));
				ship.setDWT(Integer.parseInt(rs.getString("dwt")));
				ship.setPowerKw(Integer.parseInt(rs.getString("powerkw")));
				ship.setType(rs.getString("type_en"));
				records.add(ship);
			}			
			conn.close();
		} catch (SQLException e) {
			System.out.println("查询数据失败" + e.getMessage());
		}	
		return records;
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
	
	// count the number of ais messages for each ship in the year of 2012
	public static void countEachShipAisMsg(List<Ship> ships) throws TTransportException, TException, ClientException{
		
		String mmsi;
		String hql;
		int shipsSize=ships.size();
		HqlResult2 aisRcd;
		int count=0;
	    System.out.println(new java.util.Date());
		for (int i=0;i<shipsSize-1;i++){	
			mmsi=ships.get(i).getMMSI();
			hql="select * from t41_ais_history where row=^" + "'"
					+ mmsi + "'"
					+ "and '2013-01-01' > TIMESTAMP > '2012-01-01'";
			aisRcd=hqlQuery(hql);
			count=aisRcd.cells.size(); // the output number is set to be less then Integer.MAX_VALUE=2147483647
			System.out.println("ship mmsi: " + mmsi + "  count:*******"+count);	
		}
		System.out.println(new java.util.Date());		
	}

}




		
	
	
	


