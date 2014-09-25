package individualShip;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.HqlResult2;

public class AisDataProcess {
	
	private String hypertableHost="192.168.9.176";
	private int hypertablePort=38080;
	private String namespace="/aisdb";
	private String dbDriverName="com.mysql.jdbc.Driver";
	private String dbUrl="jdbc:mysql://192.168.9.202:3306/boloomodb";
	private String dbUser="root";
	private String dbPassword="wE32v1Zqy";
	
	public HqlResult2 getAisData(String hql) throws Exception{
		
		HqlResult2 aisRecords = null;
		ThriftClient client=this.connectHypertable();
		long ns = client.open_namespace(namespace);
		aisRecords = client.hql_query2(ns, hql);
		client.close();
		return aisRecords;
		
	}
	
	public List<AisPoint> processAisData(String hql) throws Exception {

		AisPoint aisPoint = new AisPoint();
		List<AisPoint> aisPoints=new ArrayList<AisPoint>();
		HqlResult2 results = this.getAisData(hql);

		List<List<String>> aisRcds = results.cells;
        //extract ais data from each cells
		for (int i = 0; i < aisRcds.size(); i++) {
			List<String> record = aisRcds.get(i);
			aisPoint = extractAIS(record);
			aisPoints.add(aisPoint);
	
		}
		
		return aisPoints;
	
	}
	
	public void outputAisDate(List<AisPoint> points,BufferedWriter bw) throws IOException{
		
		String line;
		AisPoint point = new AisPoint();
		//排序
		Collections.sort(points);
       
		for (int i = 0; i < points.size(); i++) {
			point = points.get(i);
			line =  point.getMMSI()+" "
					+point.getTime() + " "
					
					+ point.getActualSpeed() + " " + point.getLon() + " "
					+ point.getLat()+ " " +point.getStatus();
			bw.write(line);
			bw.newLine();
		}
	}
	
	// extract a single ais messages
	public AisPoint extractAIS(List<String> record) {
		// key: 0,mmsi;1,timestamp;
		//value: 2,nav_status;3,rot;4,sog;5,pos_acc;6,lat;7,lon;
		// 8,cog;9,true_head;10,eta;11,dest_id;12,dest��13,src_id;14,dist_to_port;15,blm_avg_speed
		AisPoint point = new AisPoint();
		
		String[] key = record.get(0).split(" ");
		String[] value = record.get(3).split("@");
		point.setMMSI(key[0]);
		point.setTime(Long.parseLong(key[1]));
		point.setStatus(value[0]);
		point.setActualSpeed(Integer.parseInt(value[2]));
		point.setLon(Float.parseFloat(value[4]));
		point.setLat(Float.parseFloat(value[5]));
		return point;
		
		
	}
	
	
	public ThriftClient connectHypertable() throws Exception, TException{
	
		ThriftClient client = ThriftClient.create(hypertableHost, hypertablePort);
		return client;
	}
	public Connection connectMysql() throws ClassNotFoundException, SQLException{
		
		Connection conn = null; 
		Class.forName(dbDriverName);
		conn = DriverManager.getConnection(dbUrl,dbUser,dbPassword);
		return conn; 
		
	}

}
