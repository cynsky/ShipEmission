package individualShip;

public class AisPoint  implements Comparable<AisPoint> {
	private String mmsi;
	private String status;
	private Long time;
	private int actualSpeed;
	private float lon;
	private float lat;
	
	//http://www.blogjava.net/landor2004/articles/sort.html
	public int compareTo(AisPoint point) {
		return this.getTime().compareTo(point.getTime());
	}
	
	public void setMMSI(String mmsi){
		this.mmsi=mmsi;
	}
	public void setStatus(String status){
		this.status=status;
	}
	public void setTime(long time){
		this.time=time;
		
	}
	public void setActualSpeed(int speed){
		this.actualSpeed=speed;
	}
	public void setLon(float lon){
		this.lon=lon;
	}
	public void setLat(float lat){
		this.lat=lat;
	}
	
	public String getMMSI(){
		return this.mmsi;	
	}
	public String getStatus(){
		return this.status;
	}
	public Long getTime(){
		return this.time;
	}
	public int getActualSpeed(){
		return this.actualSpeed;
	}
	public float getLon(){
		return this.lon;
	}
	public float getLat(){
		return this.lat;
	}
	
	
	
	
}
