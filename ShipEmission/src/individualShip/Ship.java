package individualShip;

public class Ship {
	
	
	private String mmsi;
	private double serviceSpeed;
	private int powerKw;
	private int dwt;
	private String type;
	
	// add ship built year
	
	public Ship(){
		
	}
	public void setMMSI(String mmsi){
		this.mmsi=mmsi;
		
	}
	
	public void setDesignSpeed(double speed){
		this.serviceSpeed=speed;
	}
	
	public void setPowerKw(int power){
		this.powerKw=power;
	}
	
	public void setDWT(int dwt){
		this.dwt=dwt;
	}
	
	public void setType(String type){
		this.type=type;
	}
	
	public String getMMSI(){
		return this.mmsi;
	}
	
	public double getSpeed(){
		return this.serviceSpeed;
	}
	
	public int getPower(){
		return this.powerKw;	
	}
	
	public int getDWT(){
		return this.dwt;
		
	}
	
	public String getType(){
		return this.type;
	}

}
