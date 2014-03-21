import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
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
	static String querySql = "select shipid,mmsi,speed,powerkw,dwt,type_en from shipview "
			+ "where mmsi is not null and powerkw >0 and type_en is not null and type_en='container'";

	static String containerQuerySql = "select shipid,mmsi,speed,powerkw,dwt,type_en from ships "
			+ "where mmsi is not null and speed>0 and dwt>0 and powerkw>0 and type_en='container'";

	static ThriftClient client;

	public static void main(String[] args) throws TTransportException,
			TException, ClientException, IOException, SQLException {

		// get ship register records
		client = ThriftClient.create("192.168.9.175", 38080);
		List<Ship> ships = queryShips(querySql);

		//saveMultipleShipTrajectories();
		createSql();//test ucl imos
		//countShipAISMsg(containerQuerySql);

		// get ais records from hypertable based on ship mmsi

		// String mmsi=ships.get(500).getMMSI();
		// String hql = "select * from t41_ais_history where row=^" + "'"
		// + mmsi + "'"
		// + "and '2013-01-01' > TIMESTAMP > '2012-01-01'";

		String mmsi = "413623000";
		String hql = "select * from t41_ais_history where row=^"
				+ "'"
				+ mmsi
				+ "' and '2013-10-28 12:30:00' > TIMESTAMP > '2013-10-24 02:00:00'";

		aisRcd = hqlQuery(hql);
		int count = aisRcd.cells.size(); // the output number is set to be less
											// then Integer.MAX_VALUE=2147483647
		System.out.println("mmsi: " + mmsi + " count: " + count);

		// compress the trajectory based on speed,latitude and longtitude
		// distances btn two points
		System.out.println(new java.util.Date()
				+ "-----------start compress------------");
		List<GeoPoint> originalShape = new ArrayList<GeoPoint>();
		List<GeoPoint> newShape = new ArrayList<GeoPoint>();
		double tolerance = 1;
		List<String> point = new ArrayList<String>();

		for (int i = 0; i < aisRcd.cells.size(); i++) {
			List<String> record = aisRcd.cells.get(i);
			point = extractAIS(record);
			originalShape.add(new GeoPoint(point.get(0), Long.parseLong(point
					.get(1)), Double.parseDouble(point.get(4)), Double
					.parseDouble(point.get(6)),
					Double.parseDouble(point.get(7)), Double.parseDouble(point
							.get(8))));
			newShape = AISTrajectoryCompress.reduceWithTolerance(originalShape,
					tolerance);
		}
		System.out.println("compress result:  " + " origin: "
				+ originalShape.size() + " new: " + newShape.size()
				+ "----------");

		// deal with compressed data,namely, newshape. calculate the statistic
		// measurements of GeoLine and save to files.
		System.out
				.println(new java.util.Date()
						+ "-----------start save data to files and hypertable------------");
		List<GeoPoint> records = new ArrayList<GeoPoint>();
		records = newShape; // use the compressed shape
		GeoPoint endPoint = null;

		// Ship ship=ships.get(500);//a specific ship

		Ship ship = new Ship();
		ship.setDesignSpeed(12);
		ship.setDWT(22659);
		ship.setMMSI(mmsi);
		ship.setPowerKw(4400);
		ship.setType("general cargo");

		GeoPoint startPoint = newShape.get(0);
		// geoline info write to aisline.txt
		FileWriter fw = new FileWriter("e:/outputs/aisline_" + mmsi + ".txt");// 创建FileWriter对象，用来写入字符流
		BufferedWriter bw = new BufferedWriter(fw); // 将缓冲对文件的输出
		// grid emissions write to gridEms.txt
		FileWriter gridWriter = new FileWriter("e:/outputs/gridEms_" + mmsi
				+ ".txt");// 创建FileWriter对象，用来写入字符流
		BufferedWriter bgw = new BufferedWriter(gridWriter); // 将缓冲对文件的输出
		for (int i = 1; i < newShape.size(); i++) {
			endPoint = newShape.get(i);
			GeoLine line = new GeoLine(startPoint, endPoint, ship);
			if (i == 7625) {
				System.out.println("distance" + line.distance() + "speed:"
						+ line.avgSpeed() + "mainload:" + line.mainLoadFactor()
						+ "sLon:" + line.getStartPoint().longitudeE6 + "slat:"
						+ line.getStartPoint().latitudeE6 + "elon:"
						+ line.getEndPoint().longitudeE6 + "elat:"
						+ line.getEndPoint().latitudeE6);
			}
			line.saveToFile(bw);
			line.gridEmsToFile(bgw);
			// System.out.println("start savelineto ht i: " + i + " date: " +new
			// java.util.Date());
			// hqlSaveLine(line); //save line meaurements to hypertable ais in
			// namespace '/wzh'
			// System.out.println("start savegrid to ht i: " + i + " date: "
			// +new java.util.Date());
			// hqlSaveGrid(line); // save grid measurenmts to hypertable grid in
			// namespavce '/wzh'

			startPoint = endPoint;

		}
		System.out
				.println(new java.util.Date()
						+ "-----------end save data to files and hypertable------------");
		client.close();
		bw.flush(); // 刷新该流的缓冲
		bw.close();
		fw.close();

		bgw.flush();
		bgw.close();
		gridWriter.close();
	}

	// get ais messages from hypertable

	public static HqlResult2 hqlQuery(String hql) throws TTransportException,
			TException, ClientException {

		ThriftClient client = ThriftClient.create("192.168.9.175", 38080);
		long ns = client.open_namespace("/aisdb");
		HqlResult2 ais;
		ais = client.hql_query2(ns, hql);
		client.close();
		return ais;
	}

	// extract a single ais messages
	static List<String> extractAIS(List<String> record) {
		// 0,mmsi;1,timestamp;2,nav_status;3,rot;4,sog;5,pos_acc;6,lat;7,lon;
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

	// get ships from mysql

	public static List<Ship> queryShips(String sql) {
		/**
		 * SELECT * FROM shipview WHERE mmsi IS NOT NULL; DROP VIEW shipview;
		 * CREATE VIEW shipview AS SELECT ship.shipid shipid,ship.mmsi
		 * mmsi,ship.IMO imo,ship.CALLSIGN,ship.speed speed,eng.powerkw
		 * powerkw,ton.dwt dwt,tp.key1 typecode,tp.desc_en type_en,tp.desc_cn
		 * type_cn, ship.name shipname,ship.localname
		 * localname,ship.country_code country,ship.built built_date,ship.owner
		 * ship_owner, ton.grosston grosston, ton.netton netton,eng.builder
		 * builder,eng.number number,eng.rpm rpm,eng.stroke stroke, dim.loa
		 * max_length, dim.maxbeam beam, dim.draft draft FROM t41_ship ship
		 * INNER JOIN t41_ship_engine eng ON ship.shipid=eng.shipid INNER JOIN
		 * t41_ship_tonnage ton ON ship.shipid=ton.shipid INNER JOIN
		 * t41_ship_dimension dim ON ship.shipid=dim.shipid INNER JOIN t91_code
		 * tp ON ship.shiptype_key=tp.key1 WHERE dwt>=100 AND speed>0;
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
	public static void countEachShipAisMsg(List<Ship> ships)
			throws TTransportException, TException, ClientException {

		String mmsi;
		String hql;
		int shipsSize = ships.size();
		HqlResult2 aisRcd;
		int count = 0;
		System.out.println("ships size: " + shipsSize);
		System.out.println("start at:" + new java.util.Date());
		for (int i = 0; i < shipsSize - 1; i++) {
			System.out.println("start time:" + new java.util.Date());
			mmsi = ships.get(i).getMMSI();
			hql = "select * from t41_ais_history where row=^" + "'" + mmsi
					+ "'" + "and '2013-01-01' > TIMESTAMP > '2012-01-01'";
			aisRcd = hqlQuery(hql);
			count = aisRcd.cells.size(); // the output number is set to be less
											// then Integer.MAX_VALUE=2147483647
			System.out.println(mmsi + " " + count);
			System.out.println("end time:" + new java.util.Date());
		}
		System.out.println("end at:" + new java.util.Date());
	}

	public static void hqlSaveLine(GeoLine line) throws TException, TException,
			ClientException {

		// String insertHql="insert into ais values"
		// +"('"+rcd.get(1)+"','"+rcd.get(10)+"','ais','"+rcd.get(2)+"#"+rcd.get(3)+"#"+rcd.get(4)+"#"+rcd.get(5)+"#"+rcd.get(6)+"#"+rcd.get(7)+"#"
		// +rcd.get(8)+"#"+rcd.get(9)+"')";

		String insertHql = "insert into ais values" + "('"
				+ longStrToDateStr(line.getStartPoint().timestamp) + "','"
				+ line.getEndPoint().mmsi + " "
				+ line.getStartPoint().timestamp + "','ais','"
				+ line.getEndPoint().sog + "#" + line.getStartPoint().sog + "#"
				+ line.timeSpan() + "#" + line.distance() + "#"
				+ line.getStartPoint().lon + "#" + line.getEndPoint().lon + "#"
				+ line.getStartPoint().lat + "#" + line.getEndPoint().lat + "#"
				+ line.avgSpeed() + "#" + line.co2Emission() + "#"
				+ line.getShip().getType() + "')";
		String tableSchema = "<Schema><AccessGroup name='default'><ColumnFamily><Name>ais</Name><Counter>false</Counter><MaxVersions>1</MaxVersions><deleted>false</deleted></ColumnFamily></AccessGroup></Schema>";
		long ns = client.open_namespace("/wzh");
		// 当table 不存在时，程序自动新建table。但不建议采用这种方式，可以直接用ht shell新建。
		if (client.exists_table(ns, "ais") == false) {
			// create table:create table ais(ais MAX_VERSIONS=1,ACCESS GROUP
			// default(ais));
			client.create_table(ns, "ais", tableSchema);
		}
		client.hql_exec2(ns, insertHql, false, false);
	}

	public static void hqlSaveGrid(GeoLine line) throws TException, TException,
			ClientException {

		String gridIds = line.getGridIds();
		// System.out.println("gridids: " + gridIds);
		String[] unitEmission = gridIds.split("@");// ����������@123_1212
													// 0.3@123_1212 0.3
		// System.out.println("*******gridIds:"+gridIds+"**************");
		String[] gridIdEms = new String[2];
		double percent = 0;
		String gridId = null;
		String insertHql = null;
		DecimalFormat ft = new DecimalFormat("###0.0");// ����������double
														// 123.123������ 123
		for (int i = 1; i < unitEmission.length; i++) {// i=0��������

			gridIdEms = unitEmission[i].split(" ");
			gridId = gridIdEms[0];
			percent = Double.parseDouble(gridIdEms[1]);

			insertHql = "insert into grid values" + "('"
					+ longStrToDateStr(line.getStartPoint().timestamp) + "','"
					+ gridId + " " + line.getStartPoint().mmsi + " "
					+ line.getStartPoint().timestamp + "','grid','"
					+ gridId.split("_")[0] + "#" + gridId.split("_")[1] + "#"
					+ ft.format(line.avgSpeed()) + "#" + line.timeSpan() + "#"
					+ ft.format(line.mainEmission() * percent) + "#"
					+ ft.format(line.auxEmission() * percent) + "#"
					+ ft.format(line.boilerEmission() * percent) + "#"
					+ ft.format(line.co2Emission() * percent) + "#"
					+ ft.format(line.mainLoadFactor() * 100) + "')";

		}

		String tableSchema = "<Schema><AccessGroup name='default'><ColumnFamily><Name>grid</Name><Counter>false</Counter><MaxVersions>1</MaxVersions><deleted>false</deleted></ColumnFamily></AccessGroup></Schema>";
		long ns = client.open_namespace("/wzh");
		// 当table 不存在时，程序自动新建table。但不建议采用这种方式，可以直接用ht shell新建。
		if (client.exists_table(ns, "grid") == false) {
			// create table:create table grid(grid MAX_VERSIONS=1,ACCESS GROUP
			// default(grid));
			client.create_table(ns, "grid", tableSchema);
		}
		client.hql_exec2(ns, insertHql, false, false);
	}

	// timestamp from long to Date string
	public static String longStrToDateStr(long timestamp) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// 前面的lSysTime是秒数，先乘1000得到毫秒数，再转为java.util.Date类型
		java.util.Date dt = new java.util.Date(timestamp * 1000);
		String sDateTime = sdf.format(dt); // 得到精确到秒的表示：08/31/2006 21:08:00
		// System.out.println(sDateTime);
		return sDateTime;

	}

	public static void countShipAISMsg(String shipQuerySql)
			throws SQLException, TTransportException, TException,
			ClientException, IOException {

		ResultSet rs=null ;
		Connection conn;
		Statement st;
		HqlResult2 aisRcd;
		String hql, mmsi, readLine, sDate, eDate;
		conn = getConnection(); // 同样先要获取连接，即连接到数据库
		st = conn.createStatement(); // 创建用于执行静态sql语句的Statement对象，st属局部变量
		rs = st.executeQuery(shipQuerySql); // 执行sql查询语句，返回查询数据的结果集

       
        int count = 0;
		int i = 0;
		List<String> startPoint, endPoint;
		FileWriter fw;
		fw = new FileWriter("e:/outputs/ucl_imos.txt");
		// 创建FileWriter对象，用来写入字符流
		BufferedWriter cbw = new BufferedWriter(fw); // 将缓冲对文件的输出
       
		
//try{
		while (rs.next()) {
			i++;

			mmsi = rs.getString("mmsi");
			
//			hql = "select * from t41_ais_history where row=^" + "'" + mmsi
//					+ "'" + "and '2013-01-01' > TIMESTAMP > '2013-01-01'";
			hql = "select * from t41_ais_history where row=^" + "'" + mmsi
					+ "'" + "and '2013-12-01' > TIMESTAMP > '2013-01-01'";
			aisRcd = hqlQuery(hql);
			count = aisRcd.cells.size(); // the output number is set to be less
											// then Integer.MAX_VALUE=2147483647
			
			
			if (count > 0) {
				startPoint = aisRcd.cells.get(0);
				endPoint = aisRcd.cells.get(count - 1);
				startPoint = extractAIS(startPoint);
				endPoint = extractAIS(endPoint);

				SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyy-MM-dd HH:mm:ss");
				sDate = sdf.format(new Date(
						Long.parseLong(startPoint.get(1)) * 1000));
				eDate = sdf.format(new Date(
						Long.parseLong(endPoint.get(1)) * 1000));
				readLine = mmsi + " " + count + " " + sDate + " " + eDate;
				cbw.write(readLine);
				cbw.newLine();
				// System.out.println(myreadline);//
				System.out.println(rs.getString("imo") + " " + mmsi + " " + count + " " + sDate
						+ " " + eDate);
			}
		}

		cbw.flush();
		//System.out.println("end:" + new java.util.Date());
		conn.close();
		cbw.close();


	}
	
	
// calculate all container ships
	public static void saveMultipleShipTrajectories()
			throws TTransportException, TException, ClientException,
			IOException {
		String shipQuerySql = "select shipid,mmsi,speed,powerkw,dwt,type_en from ships "
				+ "where mmsi is not null and speed>0 and dwt>0 and powerkw>0 and type_en='container'";
		String trajectoryQueryHql, mmsi;

		List<Ship> ships = queryShips(shipQuerySql);
		Ship ship;
		HqlResult2 hRst;
		GeoPoint endPoint;
		GeoPoint startPoint;
		GeoLine line;
		List<GeoPoint> oriShape = new ArrayList<GeoPoint>();
		List<GeoPoint> newShape = new ArrayList<GeoPoint>();
		double tolerance = 2;
		List<String> point = new ArrayList<String>();

		if (ships.size() > 0) {
			

			for (int i = 0; i < ships.size(); i++) {

				hRst = null;
				
				
				ship = ships.get(i);
				mmsi = ship.getMMSI();
				trajectoryQueryHql = "select * from t41_ais_history where row=^"
						+ "'"
						+ mmsi
						+ "'"
						+ "and '2013-01-01' > TIMESTAMP > '2012-01-01'";
				hRst = hqlQuery(trajectoryQueryHql);
				if (hRst.cells.size() > 2) {
					
					System.out.println(mmsi +" extract and compress start:  "+new java.util.Date());
					for (int k = 0; k < hRst.cells.size(); k++) {

						point = extractAIS(hRst.cells.get(k));
						GeoPoint gPoint= new GeoPoint(point.get(0), Long
								.parseLong(point.get(1)), Double
								.parseDouble(point.get(4)), Double
								.parseDouble(point.get(6)), Double
								.parseDouble(point.get(7)), Double
								.parseDouble(point.get(8)));
						oriShape.add(gPoint);
						newShape = AISTrajectoryCompress.reduceWithTolerance(
								oriShape, tolerance);

					}

					System.out.println(mmsi +  new java.util.Date() + " compress finish and start save files:  "
							+ " origin: " + oriShape.size() + " new: "
							+ newShape.size());
					// deal with compressed data,namely, newshape. calculate the
					// statistic measurements of GeoLine and save to files.
					

					FileWriter fw = new FileWriter(
							"e:/outputs/container/aisline_" + mmsi + ".txt");// 创建FileWriter对象，用来写入字符流
					BufferedWriter bw = new BufferedWriter(fw); // 将缓冲对文件的输出
					// grid emissions write to gridEms.txt
					FileWriter gridWriter = new FileWriter(
							"e:/outputs/container/gridEms_" + mmsi + ".txt");// 创建FileWriter对象，用来写入字符流
					BufferedWriter bgw = new BufferedWriter(gridWriter); // 将缓冲对文件的输出

					startPoint = newShape.get(0);
					// geoline info write to aisline.txt
					int count=0,savedRecords=0;
					double speed=0;
					try{
						
					
					for (int j=1;j < newShape.size(); j++) {
						endPoint = newShape.get(j);
						line= new GeoLine(startPoint, endPoint, ship);
						speed=line.avgSpeed()*0.95;
						if(speed>ship.getSpeed()*1.2){ //delete point with much bigger than ship design speep
							
							System.out.println("j"+j+"  " +"ship speed: "+Math.round(ship.getSpeed()) +" actual speed:"+Math.round(speed/0.95) + "time: "+line.timeSpan()/3600
									+" dist "+Math.round(line.distance())+"first sog: "+line.getStartPoint().sog + "second sog" + line.getEndPoint().sog);
							
							continue; //skip this step
							
							
						}else if(ship.getSpeed()<speed && speed <=ship.getSpeed()*1.2){ //delete point with much bigger than ship design speep
							line.setSpeed(ship.getSpeed()); //set the line speed to the max speed;
							
						}
							
				
						
						line.saveToFile(bw);
						line.gridEmsToFile(bgw);
						startPoint = endPoint;
						
						count=j;
						savedRecords++;

					}
					System.out.println(mmsi + " " +new java.util.Date() + " end data save,count= "+count+ " records saved");
					}catch(Exception e){
						System.out.println(mmsi + " in catch clause, count= "+count+"---- "+new java.util.Date());
						
						
						e.printStackTrace();
						
					}finally{
						System.out.println(mmsi + " in finally clause, savedRecords= "+savedRecords+"---- "+new java.util.Date());
						bw.flush(); // 刷新该流的缓冲
						bw.close();
						fw.close();
						bgw.flush();
						bgw.close();
						gridWriter.close();
						
					}
					

				}

			}

		}

	}

public static String createSql(){
	String sql="";
//	// only for ucl test
	String ucl = "8727903,9510515,9152832,8700046,9286621,9085340,9156773,9432464,9380049,9434539,9088548,9409209,8888070,9454527,9403205,9254630,9286229,9476812,8712764,9045534,7304912,9002556,9501617,9359117,9138240,9448047,7128095,9403774,9175080,9445368,9356103,6703343,9514559,9381524,9085730,9395886,9015539,9634892,9550266,9291341,9385142,9450909,9548225,9423578,9174725,7501871,9585314,8201894,9341378,9321031,9544750,9128128,9461881,9109914,9525340,9059119,8138906,9157686,8608030,9125994,9256353,9067556,9180231,8113554,9162760,8419142,9396763,9082609,9390161";
    String[] imos=ucl.split(",");
    System.out.println(imos.length);
    for(int j=0;j<imos.length;j++){
    	
    	sql="SELECT mmsi,imo FROM t41_ship WHERE imo='"+imos[j]+"'";
    	try {
			countShipAISMsg(sql);
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
   
	return sql;
}


}
