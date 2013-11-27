import java.text.DecimalFormat;
import java.util.Arrays;
import java.io.*;

/**
 * @author Administrator
 * 
 */
public class GeoLine {
	private GeoPoint startPoint;
	private GeoPoint endPoint;
	private Ship ship;
	private double speed;
	private double spanTime;
	private double distance;
	static DecimalFormat ft = new DecimalFormat("#########.#");//����������double 123.123������ 123 
	static int enlarge=10;//scale for the unit grid o.1*0.1 if enlarge =100 0.01*0.01	

	public GeoLine(GeoPoint start, GeoPoint end, Ship ship) {
		this.startPoint = start;
		this.endPoint = end;
		this.ship = ship;
		this.distance = this.distance();
		this.spanTime = this.timeSpan();
		this.speed = this.avgSpeed();

	}

	public GeoPoint getStartPoint() {
		return this.startPoint;
	}

	public GeoPoint getEndPoint() {
		return this.endPoint;
	}

	public Ship getShip() {
		return this.ship;
	}

	public double timeSpan() {
		double timeSpan = Math.abs(this.endPoint.timestamp
				- this.startPoint.timestamp);

		return timeSpan;
	}

	// the result of this method seems a little bit bigger than the method in GeoPoint.java
	// however, the later one has product a result with NaN in line 7625, which could not be read by R program
	public double distance() {
		 double PI = Math.PI;
		 double R = 6378137; // meters
		 double cLonR = this.endPoint.lon * PI / 180;
		 double cLatR = this.endPoint.lat * PI / 180;
		 double pLonR = this.startPoint.lon * PI / 180;
		 double pLatR = this.startPoint.lat * PI / 180;
		 double x = (cLonR - pLonR) * Math.cos((cLatR + pLatR) / 2);
		 double y = cLatR - pLatR;
		 double distance = Math.pow(Math.pow(x, 2) + Math.pow(y, 2), 0.5) * R/
		 1852; //1855 or 1852 ,china 1852 ,england:1855, my calculation in lon=0,lat from 0 to 0.1 1855
		 return distance;
//		double distance = this.endPoint.distanceTo(this.startPoint)/1852;
//		return distance;
	}

	public double avgSpeed() {
		int timeTolerance = 600; // s
		int distanceTolerance=1; //nm
		double avg_speed = 0;
		// compute average speed between two points
		if (this.timeSpan() <= timeTolerance && this.distance()<distanceTolerance) {
			avg_speed = 0.5 * 0.1 * (this.startPoint.sog + this.endPoint.sog);
		} else {

			avg_speed = this.distance()/this.spanTime * 3600;
		}
		return avg_speed;

	}

	
	public double co2Emission() {
		return Math.round(this.mainEmission() + this.auxEmission()
				+ this.boilerEmission()); 

	}

	// ��������
	public double mainEmission() {
		
		double emission = 0;
		double timeSpan = this.spanTime / 3600;// ����
		double powerkw = this.ship.getPower();
		
		emission = powerkw * timeSpan * this.mainLoadFactor()
				* this.mainEmFactor();

		return Math.round(emission);

	}

	// ��������
	public double auxEmission() {
		// ��������
		double emission = 0;

		double timeSpan = this.spanTime / 3600;// ����
		double powerkw = this.ship.getPower();
		double engFactor = 0.220;// aux engine power/main engine power =0.220
		double auxPower = powerkw * engFactor;

		
		emission = auxPower * timeSpan * this.auxLoadFactor()
				* this.auxEmFactor();

		return Math.round(emission);

	}

	public double boilerEmission() {
		// ��������
		double eFactor = 922.97;

		double timeSpan = this.spanTime / 3600;// ����
		int boilerEnergy = 506;
		double emission = 0;
		// ������maveuring �� hotelling����������Boiler��������
		if (this.speed < 8) {

			emission = boilerEnergy * timeSpan * eFactor;

		}

		return Math.round(emission);

	}

	public double fuelConsumption() {
		// assume:type:container, oil:RO
		double fuel = 0;
		// double SFOC= 213;// ����������HFO(RO), SFOC=213g/kw.h
		double design_speed = this.ship.getSpeed();
		double timeSpan = this.spanTime / 3600;// ����
		double powerkw = this.ship.getPower();
		double engFactor = 0.220;// aux engine power/main engine power =0.220
		double auxPower = powerkw * engFactor;

		// ������������������
		fuel = powerkw * timeSpan * this.mainLoadFactor() * this.mainBSFC()
				+ auxPower * timeSpan * this.auxLoadFactor() * this.auxBSFC();
		// ��������������������������������������������������
		System.out.println("design_speed: " + design_speed + "avgSpeed: "
				+ this.avgSpeed() + "power:" + powerkw + "sfoc:213"
				+ "fuel consumption: " + Math.round(fuel));
		return Math.round(fuel);

	}

	public String getGridIds() {
		// ������������������0.1X0.1������������360��������180����������������������1800X3600��������
		// ������������gridIds������gridId����������������������gridId��@����������gridId��grid ����������
		// lat*10_lon*10_��������grid��������������
		double cLat = this.endPoint.lat; // clat =current lat
		double cLon = this.endPoint.lon;
		double pLat = this.startPoint.lat;
		double pLon = this.startPoint.lon;

		String gridIds = "";
		int enlarge = 10; // ����������������������������100�� ����������������0.01X0.01����������10 ��
							// 0.1X0.1

		int cLatFloor = (int) Math.floor(cLat * enlarge);
		int pLatFloor = (int) Math.floor(pLat * enlarge);
		int cLonFloor = (int) Math.floor(cLon * enlarge);
		int pLonFloor = (int) Math.floor(pLon * enlarge);

		int latSpan = (int) Math.floor(cLat * enlarge)
				- (int) Math.floor(pLat * enlarge);// ��������������������(+1 ������
		int lonSpan = (int) Math.floor(cLon * enlarge)
				- (int) Math.floor(pLon * enlarge);// ��������������������(+1 ������
		double percent = 0;// ������grid��������

		// System.out.println("lonspan: " + lonSpan + "   latspan: " + latSpan
		// + "   point_lat:" + Math.floor(cLat * enlarge) + "   point_lon:"
		// + Math.floor(cLon * enlarge));

		// ����������������
		if (cLon == pLon) {// ����������������������������

			if (Math.abs(latSpan) > 0) {// ������������������

				for (int i = 0; i <= Math.abs(latSpan); i++) {

					// ������grid����������,����i������i����������i��0��������0����������������
					if (i == 0) {
						percent = (Math.min(cLatFloor, pLatFloor) + 1 - Math
								.min(cLat, pLat) * enlarge)
								/ (Math.abs(cLat - pLat) * enlarge);
					} else if (i == Math.abs(latSpan)) {
						percent = (Math.max(cLat, pLat) * enlarge - (Math.min(
								cLatFloor, pLatFloor) + i))// ��������Math.max(cLatFloor,
															// pLatFloor)
								/ (Math.abs(cLat - pLat) * enlarge);
					} else {
						percent = 1 / (Math.abs(cLat - pLat) * enlarge);
					}
					// gridIds
					gridIds = gridIds + "@" + cLonFloor + "_"
							+ (Math.min(cLatFloor, pLatFloor) + i) + " "
							+ percent;
				}
			} else {
				gridIds = gridIds + "@" + (int) cLonFloor + "_"
						+ (int) cLatFloor + " " + 1;
			}

		} else {

			// get the line equation
			double k = (cLat - pLat) / (cLon - pLon);
			double b = cLat * enlarge - k * cLon * enlarge;
			// double x = 0;
			// double y=k*x+b;
			double[] lats = new double[Math.abs(latSpan) + 1];// ��������������������
			double[] lons = new double[Math.abs(lonSpan) + 1];// ��������
			double[] y_lats = new double[Math.abs(lonSpan) + 1];// ?
			double[] x_lons = new double[Math.abs(latSpan) + 1];// ?
			double[] point_lons = new double[Math.abs(latSpan)
					+ Math.abs(lonSpan) + 2]; // ����������������������������������������������������������������������grid��

			point_lons[0] = Math.min(cLon * enlarge, pLon * enlarge);// ��0������lon����������������lon����
			point_lons[Math.abs(latSpan) + Math.abs(lonSpan) + 1] = Math.max(
					cLon * enlarge, pLon * enlarge);

			// get the lat direct crosses
			if (Math.abs(latSpan) > 0) {
				for (int j = 1; j <= Math.abs(latSpan); j++) {
					lats[j] = Math.min(cLatFloor, pLatFloor) + j;
					x_lons[j] = (lats[j] - b) / k;
					point_lons[j] = x_lons[j];// ��j����lon����

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

			double midPointLon = 0;
			double midPointLat = 0;
			double pcent = 0;
			// create gridIds
			for (int j = 0; j < point_lons.length - 1; j++) {

				midPointLon = 0.5 * (point_lons[j] + point_lons[j + 1]);// ������������������������gridId
				midPointLat = k * midPointLon + b;
				pcent = Math.abs((point_lons[j + 1] - point_lons[j])
						/ (point_lons[point_lons.length - 1] - point_lons[0]));

				gridIds = gridIds + "@" + (int) Math.floor(midPointLon) + "_"
						+ (int) Math.floor(midPointLat) + " " // ��������gridId ��
																// emission
																// percent ����
						+ pcent;

			}

		}

		return gridIds;

	}

	public double mainLoadFactor() {

		double factor = 0.0;
		double load = Math.pow(this.avgSpeed() / this.ship.getSpeed(), 3);
		if (load < 0.02 && this.speed >=0.5) { // ����ICF 2009�� ����load factor ��0.02
			factor = 0.02;

		} else {
			factor = load;

		}
		// ����������20%������������������������������������ICF����page 43
		if (factor < 0.2 && factor > 0.01) {
			factor = factor * ((44.1 / factor + 648.6) / (44.1 / 0.2 + 648.6));
		}

		return factor;

	}

	public double auxLoadFactor() {
		// just for container type
		double loadFactor = 0.0;

		// assume:
		// hotelling(0=<speed<1),maneuvering(1=<speed<9),RSZ(9-12),cruise(>12)
		if (this.speed < 1 && this.speed >= 0) {
			loadFactor = 0.19;
		} else if (this.speed < 8 && this.speed >= 1) {
			loadFactor = 0.48;

		} else if (this.speed < 12 && this.speed >= 8) {
			loadFactor = 0.25;

		} else if (this.speed >= 12) {
			loadFactor = 0.13;
		}
		return loadFactor;

	}

	public double mainEmFactor() {
		// ������������������RO��������emission factor=677.91 g/kw.h��BSFC=213g/kw.h
		double EF = 677.91;
		return EF;

	}

	public double auxEmFactor() {
		// ������������������RO��������emission factor=722.54 g/kw.h��BSFC=227g/kw.h
		double F = 227;
		return F;

	}

	public double mainBSFC() {
		// ������������������RO��������emission factor=677.91 g/kw.h��BSFC=213g/kw.h
		double F = 213;
		
		return F;

	}

	public double auxBSFC() {
		// ������������������RO��������emission factor=722.54 g/kw.h��BSFC=227g/kw.h

		double EF = 227.54;
		return EF;

	}

	// save the result to a document which could be access by R program

	public void saveToFile(BufferedWriter bw) {

		try {
            //mmsi+start_time(s)+start_sog/10(nm/h)+end_sog+start_lat+start_lon + end_lat +end_lon
			//+distance(nm)+avgspeed(nm/h)
			//+spantime(s)+main_emission(g)+ aux_emission(g)+bioler_emission(g)+ total_emission+main main_load(%)
			String myreadline = this.startPoint.mmsi + "@" 
					+ this.startPoint.timestamp + "@"
					+ this.startPoint.sog / 10 + "@"
					+ this.endPoint.sog / 10 + "@"
					+ this.startPoint.lat+ "@" 
					+ this.startPoint.lon + "@" 
					+ this.endPoint.lat + "@"
					+ this.endPoint.lon + "@" 
					+ ft.format(this.distance) + "@"
					+ ft.format(this.speed) + "@" 
					+ this.spanTime + "@"
					+ ft.format(this.mainEmission()) + "@" 
					+ ft.format(this.auxEmission())+ "@" 
					+ ft.format(this.boilerEmission()) +"@" 
					+ ft.format(this.co2Emission()) + "@" 
					+ ft.format(this.mainLoadFactor()*100);

			bw.write(myreadline);
			bw.newLine();
			//System.out.println(myreadline);// 

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void gridEmsToFile(BufferedWriter bw) throws IOException {

		String gridIds = this.getGridIds();
		// System.out.println("gridids: " + gridIds);
		String[] unitEmission = gridIds.split("@");// ����������@123_1212 0.3@123_1212 0.3
		String[] gridIdEms = new String[2];
		double percent = 0;
		String gridId = null;
		for (int i = 1; i < unitEmission.length; i++) {// i=0��������
			gridIdEms = unitEmission[i].split(" ");
			gridId = gridIdEms[0];
			percent = Double.parseDouble(gridIdEms[1]);
			// mmsi+star_time(s)+end_time(s)+scale(��������10)+gridId(lon(��)*10+lat*10) + lon*10,lat*10
			// spantime(s)+main_emission(g)+ aux_emission(g)+bioler_emission(g)+total_Emission(g)
			String myreadline = this.startPoint.mmsi + "@"
					+ this.startPoint.timestamp + "@" 
					+ this.endPoint.timestamp
					+ "@" + gridId + "@" + gridId.split("_")[0] + "@"
					+ gridId.split("_")[1] + "@"
					+ ft.format(this.speed) + "@" 
					+ this.spanTime + "@"
					+ ft.format(this.mainEmission() * percent) + "@"
					+ ft.format(this.auxEmission() * percent) + "@"
					+ ft.format(this.boilerEmission() * percent) + "@"
					+ ft.format(this.co2Emission() * percent) + "@"
					+ ft.format(this.mainLoadFactor() * 100);

			bw.write(myreadline); 
			bw.newLine();			

		}
	}
}
