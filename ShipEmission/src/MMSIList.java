import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.HqlResult2;

/**
 * aims to get the distinct mmsi list from ais hypertable database
 */

/**
 * @author zhihuan
 *
 */

public class MMSIList {


	/**
	 * @param args
	 */
	
	static HqlResult2 aisRcd; // AIS records from hypertable
	static String hql = "select * from t41_ais_history where '2013-01-01' > TIMESTAMP > '2012-01-01' limit 1000";
	
	public static void main(String[] args) throws TTransportException, TException, ClientException {
		// TODO Auto-generated method stub
		int count = 0;
		
		aisRcd=hqlQuery(hql);
		count=aisRcd.cells.size();
		
		
	

	}
	
	public static HqlResult2 hqlQuery(String hql) throws TTransportException, TException,
	ClientException {
		
		ThriftClient client = ThriftClient.create("192.168.9.175", 38080);	
        long ns =client.open_namespace("/aisdb");  
        HqlResult2 ais;
        ais=client.hql_query2(ns, hql);
        client.close();
        return ais;
	}
	
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
        

}
		
	
	
	


