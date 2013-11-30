import java.text.DecimalFormat;

/**
 * @author Administrator
 *
 */
public class Ship {
	
	private String mmsi;
	private double serviceSpeed;
	private int powerKw;
	private int dwt;
	private String type;
	private long[][] shipOut;
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
	public long[][] getShipOut(){
		return this.getShipSpeedEmissions();
	}
	public long [][] getShipSpeedEmissions(){
		long[][] out=new long[300][8];
		
		int speed=0;
		
		for (int i=0;i<300;i++){
			speed=i;
			out[i][0]=Math.round(this.fuelConsumptionRate(speed));
			out[i][1]=Math.round(this.mainFuelRate(speed));
			out[i][2]=Math.round(this.auxFuelRate(speed));
			out[i][3]=Math.round(this.boilerFuelRate(speed));
			
			out[i][4]=Math.round(this.co2EmissionRate(speed));
			out[i][5]=Math.round(this.mainCo2Emission(speed));
			out[i][6]=Math.round(this.auxCo2Emission(speed));
			out[i][7]=Math.round(this.boilerCo2Emission(speed));
			
		}
		
		
		return out;
	}
	// fuel consumption
	public double fuelConsumptionRate(int speed){
		double fuel=0;
		
		fuel=mainFuelRate(speed)+auxFuelRate(speed)+boilerFuelRate(speed);
		
		
		return fuel;
	}
	
	public double co2EmissionRate(int speed){
		double e=0;
		e=mainCo2Emission(speed)+auxCo2Emission(speed)+boilerCo2Emission(speed);
		return e;
	}
	
	public double mainFuelRate(int speed){
		double out =0;
		
		out=mainLoadFactor(speed)*this.getPower()*mainFF();
		
		return out;
		
	}
	
	
	public double mainLoadFactor(double speed){
		double out =0;
		double load =Math.pow(speed*0.95/this.getSpeed(), 3);
		if (load < 0.02 && speed >=0.5) { // ICF 2009 load factor ��0.02
			out = 0.02;

		} else {
			out = load;

		}
		//ICF page 43
		if (out < 0.2 && out > 0.01) {
			out = out * ((44.1 / out + 648.6) / (44.1 / 0.2 + 648.6));
		}

		return out;	
	}
	
	public double mainFF() {
		// TODO Auto-generated method stub
		return 213; //RO g/kw.h
		
	}
	public double auxFuelRate(int speed){
		double out =0;
		out =auxLoadFactor(speed)*auxPower()*auxFF();
		
		return out;
		
	}
	
	private double auxFF() {
		// TODO Auto-generated method stub
		return 217;//MDO 
	}
	private double auxPower() {
		// TODO Auto-generated method stub
		return 0.22*this.getPower();
	}
	public double auxLoadFactor(int speed) {
		// just for container type
		double loadFactor = 0.0;

		// assume:
		// hotelling(0=<speed<1),maneuvering(1=<speed<9),RSZ(9-12),cruise(>12)
		if (speed < 1 && speed >= 0) {
			loadFactor = 0.19;
		} else if (speed < 8 && speed >= 1) {
			loadFactor = 0.48;

		} else if (speed < 12 && speed >= 8) {
			loadFactor = 0.25;

		} else if (speed >= 12) {
			loadFactor = 0.13;
		}
		return loadFactor;

	}
	
	public double boilerFuelRate(int speed){
		double out =0;	
			
			
			if (speed < 8) {
				out = boilerEnergy()*boilerFF();
			}	
			return out;
	}
	
	
	private int boilerFF() {
		// TODO Auto-generated method stub
		return 290; //ST
	}
	//CO2 emissions
	
	public double mainCo2Emission(int speed){
		double out =0;
		
		out=mainLoadFactor(speed)*this.getPower()*mainCo2EF();
		
		return out;
		
	}
	public double mainCo2EF() {
		// TODO Auto-generated method stub
		return 677.91;//for RO
		
	}
	public double auxCo2Emission(int speed){
		double out =0;
		
		out=auxLoadFactor(speed)*0.22*this.getPower()*auxCo2EF();
		
		return out;
		
	}
	public double auxCo2EF() {
		// TODO Auto-generated method stub
		
		return 690.71;//for MDO
	}
	
	public double boilerCo2Emission(int speed){
		double out =0;
		
		if (speed < 8) {
			out = boilerEnergy()*boilerEF();
		}	
		
		
		return out;
		
	}
	public double boilerEnergy(){
		return 506;//container avg
	}
	
	private double boilerEF() {
		// TODO Auto-generated method stub
		return 922.97;
	}
	
	
	
	
	
			
	
}
