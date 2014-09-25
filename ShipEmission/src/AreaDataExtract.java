/**

 * 
 */

import it.sauronsoftware.base64.Base64;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;


/**
 * @author Administrator
 *
 */
public class AreaDataExtract {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		{
			GregorianCalendar startDate = new GregorianCalendar(2012,1,1,0,0,0); 
		      Date sd = startDate.getTime();
		     long startTime= sd.getTime()/1000;
		     GregorianCalendar endDate = new GregorianCalendar(2012,1,2,0,0,0); 
		      Date ed = endDate.getTime();
		     long endTime= ed.getTime()/1000;
		     
		      System.out.println("start date " + startTime);
//		      startTime=1394424000;
//		    		  endTime=1394427600;
		     
		      long time=startTime;
		      long et=0;
		      
		      while (time<endTime){
		    	  
		    	  
		    	  System.out.println(time);
		    	  et=time+3600;
		    	  String selectArea = "{"
							+ "startdt:"+time+","
							+ "enddt:"+et+","
							+ "area:\"121.75,30.25|121.75,31|122.75,31|122.75,30.25|121.75,30.25\","
							+ "circle:\"\","
							+ "all:1"
							+ "}";
					
					String filePath="e:/outputs/area/"+time+".txt";
					String result="";
					result=getHttpData(selectArea);
					System.out.println(result.length());
								
					//System.out.println(result);
					extractAndSaveData(result,filePath);
		    	  time=time+3600;
		    	  
		      }
	
		}

	}
	
	public static String getHttpData(String areaQuery) throws IOException{
		
		String line="";
		
		String encoded = Base64.encode(areaQuery);
		String area = "http://218.61.5.87:8080/fcgi-bin/blmcgi?cmd=0x011a&param="
				+ encoded;

		URL url = new URL(area);
		URLConnection connection = url.openConnection();
		connection.setDoOutput(true);

		BufferedReader in = new BufferedReader(new InputStreamReader(
				connection.getInputStream()));

		line=in.readLine();
		in.close();
		return line;
		
	}
	
	public static void extractAndSaveData(String inputLine,String filePath) throws IOException{
		
		FileWriter fw = new FileWriter(filePath);// 创建FileWriter对象，用来写入字符流
		BufferedWriter bw = new BufferedWriter(fw); // 将缓冲对文件的输出
		String line = inputLine.substring(1, inputLine.length()-2);
		String[] msgs=line.split("},");
		String[] cols;
		String[] keyValue;;
		String header = "time|mmsi|stat|rot|sog|pos_acc|lon|lat|cog|truehead|nm|tp|imo|cs|length|width|draught|eta|dest|iso3|dwt";
		String body="";
		
		bw.write(header);
		bw.newLine();
				
		for (int i =0; i<msgs.length;i++){
			cols=msgs[i].substring(1,msgs[i].length()-2).split("\",");
			body="";
			for(int j=0;j<cols.length;j++){
							
				keyValue=cols[j].split(":\"");
				if (keyValue.length<2){
					
					body=body+"|"+"null";
												
				}
				else{
					
					body=body+"|"+keyValue[1];
				}
	
			}
			//System.out.println(body.substring(1));
			
			body=body.substring(1);
			bw.write(body);
			bw.newLine();
	
		}
		bw.close();
	}
	
	public void saveToFile(String filePath,String line) throws IOException{
		FileWriter fw = new FileWriter(
				"e:/outputs/area/" + ".txt");// 创建FileWriter对象，用来写入字符流
		BufferedWriter bw = new BufferedWriter(fw); // 将缓冲对文件的输出
		bw.write(line);
		bw.newLine();
		
	}

}
