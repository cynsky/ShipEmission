import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.thrift.*;
import org.hypertable.thriftgen.*;

public class ShipEmissionMain {
	
	// create table shipping(ais MAXVERSIONS=1, grid MAXVERSIONS=1,INDEX ais,QUALIFIER INDEX ais,INDEX grid ) GROUP_COMMIT_INTERVAL = 50;

	// 存储hypertable中取出的AIS相关数据
	static String mmsi;
	static String timestamp;
	static String nav_status; // 航行状态
	static String rot; // 转向率
	static String sog;
	static String pos_acc; // 设备定位精度
	static String lon;
	static String lat;
	static String cog;
	static String true_head; // 船首向
	static String eta;
	static String dest_id;
	static String dest; // 目的港
	static String src_id; // 数据来源
	static String dist_to_port; // 到目的港距离
	static String blm_avg_speed; // 平均速度
	
	//采用道格拉斯算法对单船轨迹进行压缩时，点到直线距离的判断标准 tolerance
	static double tolerance=100; //distance less than 100 meters will be removed
	
	static long spanTime;//两个报告点间时间间隔，单位秒
	static double avg_speed;//平均速度
	//两个报告点间线段进过的单元格记录，数据格式：
	//单元格底角 lon*100_lat*100_percent,percent为这段距离在该单元格的比例，多个gridId用@分隔开。
	static String gridIds;
    static double emissions;
	// 用于计算连个报告点之间的时间间隔和平均速度

	static List<String> previous_record = new ArrayList<String>();
	static List<String> current_record = new ArrayList<String>();

	static Connection conn;
	static Statement st;
	static List<Ship> ships = null; // save the ship data selected from mysql
	static Ship ship;
	static HqlResult2 aisRcd = null; // AIS records from hypertable
	static String hql;
	static int enlarge = 100;// 放大倍数 0.1X0.1 其值为10，0.01X0.01 为100
	static List<String> saveRcd=new ArrayList<String>();
	static double shipTotalEms=0;
	static ThriftClient client; 
	

	public static void main(String[] args) throws TTransportException,
			TException, ClientException, SQLException, IOException {
         //geoline info write to aisline.txt
		 FileWriter fw = new FileWriter("e:/outputs/aisline.txt");//创建FileWriter对象，用来写入字符流
         BufferedWriter bw = new BufferedWriter(fw);    //将缓冲对文件的输出
         //grid emissions write to gridEms.txt
         FileWriter gridWriter = new FileWriter("e:/outputs/gridEms.txt");//创建FileWriter对象，用来写入字符流
         BufferedWriter bgw = new BufferedWriter(gridWriter);    //将缓冲对文件的输出
		
         Double double1 = 123456789.123456789;  
         DecimalFormat decimalFormat = new DecimalFormat("###0");//格式化设置  
         System.out.println("formated:  "+decimalFormat.format(double1));  
         System.out.println(double1); 
         
         
		// retrieve ships from MySql
		ships = query();
		ship=ships.get(500);
		
		System.out.println("ship mmsi **********************: "+ship.getMMSI());
		hql = "select * from t41_ais_history where row=^" + "'"
				+ ship.getMMSI() + "'"
				+ "and '2013-01-01' > TIMESTAMP > '2012-01-01' limit 1000";
		
		client = ThriftClient.create("192.168.9.175", 38080);
		//client.close();
		aisRcd = hqlQuery(hql);
		
		// 用 DP algorithm 压缩数据
		ArrayList<GeoPoint> shape = new ArrayList<GeoPoint>();
		ArrayList<GeoPoint> newShape = new ArrayList<GeoPoint>();
		List<String> point = new ArrayList<String>();
		for (int i = 0; i < aisRcd.cells.size(); i++) {
		List<String> record = aisRcd.cells.get(i);
		point=extractAIS(record);
		shape.add(new GeoPoint(point.get(0),Long.parseLong(point.get(1)),Double.parseDouble(point.get(4)),Double.parseDouble(point.get(7)),Double.parseDouble(point.get(6)),Double.parseDouble(point.get(8))));
		newShape=DouglasPeuckerReducer.reduceWithTolerance(shape, tolerance);
		}
		
		System.out.println("newshape size **********************: "+newShape.size());
		
		//对利道格拉斯算法压缩后的轨迹进行处理
	    //newShape=shape;//only for test
	    GeoPoint endPoint =null;
	    GeoPoint startPoint=newShape.get(0);
		for (int i = 1; i < newShape.size(); i++) {
			endPoint = newShape.get(i);
			GeoLine line=new GeoLine(startPoint,endPoint,ship);
			//if(line.distance()>=0.01&&line.avgSpeed()>=0.5){//实际上删除了与前一点距离小于18.52米和平均速度小于0.1的点,即静止的点
			
			// 将previous record 的值改为当前记录值,顺序不能改变
			
            gridIds=line.getGridIds();
            emissions=line.co2Emission();//单个分段的排放
            shipTotalEms=shipTotalEms+emissions; //所有轨迹分段排放之和
		
           // System.out.println("dateStr:"+longStrToDateStr(startPoint.timestamp)+"  shipTotalEms:"+shipTotalEms);
            //save to a file for R program to access, columns are separated by "@"
            line.saveToFile(bw);
            line.gridEmsToFile(bgw);
			hqlSave(line);
							
			//startPoint=(GeoPoint)endPoint.clone();
			
			startPoint=endPoint;
			//}
			
				
		}
	
		client.close();
		bw.flush();    //刷新该流的缓冲
        bw.close();
        fw.close();
			
	}

	public static List<Ship> query() {

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
		
		
		ResultSet rs = null;
		
		List<Ship> records = new ArrayList<Ship>();
		String querySql ="select shipid,mmsi,speed,powerkw,dwt,type_en from shipview where mmsi is not null and powerkw >0 and type_en is not null and type_en='container'";
		conn = getConnection(); // 同样先要获取连接，即连接到数据库
		try {
			st = conn.createStatement(); // 创建用于执行静态sql语句的Statement对象，st属局部变量
			rs = st.executeQuery(querySql); // 执行sql查询语句，返回查询数据的结果集
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

	

	// 计算两点连线跨越几个单元，并计算在每个单元所用的时间，速度都为平均速度。每个单元分配的时间或者排放为为单元中线段长度成比例。
public static String setGrid(double cLon, double cLat, double pLon,double pLat) {
		// 假设每个单元尺寸为0.1X0.1度，其中经度360度，纬度180度，故总共将地球分割为1800X3600个单元。
		// 这里的单元格gridIds为单个gridId字符窜的合成，其中单个gridId由@分开。单个gridId为grid 左下角点的
		// lat*10_lon*10_该段在该grid中长度占的比例
		
		String gridIds = "";
		

		int cLatFloor = (int) Math.floor(cLat * enlarge);
		int pLatFloor = (int) Math.floor(pLat * enlarge);
		int cLonFloor = (int) Math.floor(cLon * enlarge);
		int pLonFloor = (int) Math.floor(pLon * enlarge);

		int latSpan = (int) Math.floor(cLat * enlarge)
				- (int) Math.floor(pLat * enlarge);//维度方向跨几个单元格
		int lonSpan = (int) Math.floor(cLon * enlarge)
				- (int) Math.floor(pLon * enlarge);//经度方向跨几个单元格
		double percent = 0;// 航段在grid中的比例

		System.out.println("lonspan: " + lonSpan + "   latspan: " + latSpan
				+ "   point_lat:" + Math.floor(cLat * enlarge) + "   point_lon:"
				+ Math.floor(cLon * enlarge));

		// 用两点求直线方程
		if (cLon == pLon) {//与进度同方向，即垂直赤道方向

			if (Math.abs(latSpan) > 0) {

				for (int i = 0; i <= Math.abs(latSpan); i++) {

					// 求每个grid占的百分比
					if (i == 0) {
						percent = (Math.min(cLatFloor, pLatFloor) + 1 - Math
								.min(cLat, pLat) * enlarge)
								/ (Math.abs(cLat - pLat) * enlarge);
					} else if (i == Math.abs(latSpan)) {
						percent = (Math.max(cLat, pLat) * enlarge - (Math.min(
								cLatFloor, pLatFloor) + i))
								/ (Math.abs(cLat - pLat) * enlarge);
					} else {
						percent = 1 / (Math.abs(cLat - pLat) * enlarge);
					}
					// gridIds的字符窜
					gridIds = gridIds + "@" + cLonFloor + "_"
							+ (Math.min(cLatFloor, pLatFloor) + i) + "_"
							+ percent;
				}
			} else {
				gridIds = gridIds + "@" + (int)cLonFloor + "_" + (int)cLatFloor + "_" + 1;
			}

		} else {
			
			// get the line equation
			double k = (cLat - pLat) / (cLon - pLon);
			double b = cLat * enlarge - k * cLon * enlarge;
			//double x = 0;
			// double y=k*x+b;
			double[] lats = new double[Math.abs(latSpan) + 1];
			double[] lons = new double[Math.abs(lonSpan) + 1];
			double[] y_lats = new double[Math.abs(lonSpan) + 1];
			double[] x_lons = new double[Math.abs(latSpan) + 1];
			double[] point_lons = new double[Math.abs(latSpan)
					+ Math.abs(lonSpan) + 2];

			point_lons[0] = Math.min(cLon * enlarge, pLon * enlarge);
			point_lons[Math.abs(latSpan) + Math.abs(lonSpan) + 1] = Math.max(
					cLon * enlarge, pLon * enlarge);
			
			// get the lat direct crosses
			if (Math.abs(latSpan) > 0) {
				for (int j = 1; j <= Math.abs(latSpan); j++) {

					lats[j] = Math.min(cLatFloor, pLatFloor) + j;
					x_lons[j] = (lats[j] - b) / k;
					point_lons[j] = x_lons[j];

				}
			}

			// get the lon direct crosses
			if (Math.abs(lonSpan) > 0) {
				for (int j = 1; j <= Math.abs(lonSpan); j++) {

					lons[j] = Math.min(cLonFloor, pLonFloor) + j;
					y_lats[j] = k * lons[j] + b;
					point_lons[Math.abs(latSpan) + j] = lons[j];
				}
			}

			Arrays.sort(point_lons);
			System.out.println("number of points:" + point_lons.length);
			
			//create gridIds
			for (int j = 0; j < point_lons.length - 1; j++) {
				
				gridIds = gridIds + "@" + (int)Math.floor(point_lons[j]) + "_"
						+ (int)Math.floor(k * point_lons[j] + b) + "_"
						+ (point_lons[j + 1] - point_lons[j])
						/ (point_lons[point_lons.length - 1] - point_lons[0]);

			}

		}

		System.out.println("gridIds:" + gridIds);
		return gridIds;

	}
// 取出一条船舶注册记录
	public static HqlResult2 hqlQuery(String hql) throws TTransportException, TException,
			ClientException {

		HqlResult2 HTRst = null;	
		long ns = client.open_namespace("/aisdb");
		HTRst = client.hql_query2(ns, hql);
		return HTRst;

	}
	
	
	public static void hqlSave(GeoLine line) throws TException, TException, ClientException{
		
//		String insertHql="insert into ais values"
//		+"('"+rcd.get(1)+"','"+rcd.get(10)+"','ais','"+rcd.get(2)+"#"+rcd.get(3)+"#"+rcd.get(4)+"#"+rcd.get(5)+"#"+rcd.get(6)+"#"+rcd.get(7)+"#"
//		+rcd.get(8)+"#"+rcd.get(9)+"')";
		
		String insertHql="insert into ais values"
		+"('"+longStrToDateStr(line.getEndPoint().timestamp)+"','"
				+line.getEndPoint().mmsi+" "+line.getEndPoint().timestamp+"','ais','"
		+line.getEndPoint().sog+"#"
		+line.getStartPoint().sog+"#"
		+line.timeSpan()+"#"
		+line.distance()+"#"
		+line.getStartPoint().lon+"#"
		+line.getEndPoint().lon+"#"
		+line.getStartPoint().lat+"#"
				+line.getEndPoint().lat+"#"
		+line.avgSpeed()+"#"
				+line.co2Emission()+"#"
		+line.getShip().getType()+"')";
		
		
		String tableSchema="<Schema><AccessGroup name='default'><ColumnFamily><Name>ais</Name><Counter>false</Counter><MaxVersions>1</MaxVersions><deleted>false</deleted></ColumnFamily></AccessGroup></Schema>";
	
		long ns = client.open_namespace("/wzh");
		
		
		
		//当table 不存在时，程序自动新建table。但不建议采用这种方式，可以直接用ht shell新建。
		if(client.exists_table(ns, "ais")==false){
			// create table:create table ais(ais MAX_VERSIONS=1,ACCESS GROUP default(ais));
			client.create_table(ns, "ais", tableSchema);
		}
		
		client.hql_exec2(ns, insertHql, false, false);		
	}
	
	//将timestamp由long类型转换成Date类型
	public static String longStrToDateStr(long timestamp){
		
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//前面的lSysTime是秒数，先乘1000得到毫秒数，再转为java.util.Date类型
		java.util.Date dt = new java.util.Date(timestamp * 1000); 
		String sDateTime = sdf.format(dt);  //得到精确到秒的表示：08/31/2006 21:08:00
		//System.out.println(sDateTime);
		return sDateTime;
		
	}

	public static List<String> extractAIS(List<String> record) {

		// report中字符串对应的位置
		// 0,mmsi;1,timestamp;2,nav_status;3,rot;4,sog;5,pos_acc;6,lon;7,lat;
		// 8,cog;9,true_head;10,eta;11,dest_id;12,dest；13,src_id;14,dist_to_port;15,blm_avg_speed

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
	
}

