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
	static DecimalFormat ft = new DecimalFormat("###0.0");//��ʽ������double 123.123����Ϊ 123 

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

	public double distance() {
		// double PI = Math.PI;
		// double R = 6378137; // ����뾶������Ϊ��λ
		// // ����γ��ת��Ϊ����
		// double cLonR = this.endPoint.lon * PI / 180;
		// double cLatR = this.endPoint.lat * PI / 180;
		// double pLonR = this.startPoint.lon * PI / 180;
		// double pLatR = this.startPoint.lat * PI / 180;
		// double x = (cLonR - pLonR) * Math.cos((cLatR + pLatR) / 2);
		// double y = cLatR - pLatR;
		// double distance = Math.pow(Math.pow(x, 2) + Math.pow(y, 2), 0.5) * R/
		// 1852; // �ر��������룬�Ժ���Ϊ��λ
		// return distance;
		double distance = this.endPoint.distanceTo(this.startPoint) / 1852;// ��λ����˷�������600������
		 															// ƫС
		
		return distance;
	}

	public double avgSpeed() {
		int threshhold = 600; // ����ƽ���ٶȼ��㷽ʽ��ʱ���ٽ��Ϊ300��
		double avg_speed = 0;
		// compute average speed between two points
		if (this.timeSpan() <= threshhold) {
			avg_speed = 0.5 * 0.1 * (this.startPoint.sog + this.endPoint.sog);
		} else {

			// ����/Сʱ������ÿСʱΪ��λ
			avg_speed = this.distance / this.spanTime * 3600;
		}
		return avg_speed;

	}

	// ������Ҫ�޼����� SFOC 213g/kw.h
	public double co2Emission() {
		// double emission=0;
		// double design_speed=this.ship.getSpeed();
		// double timeSpan=this.time/3600;//Сʱ
		// double powerkw=this.ship.getPower();
		// double engFactor=0.220;//aux engine power/main engine power =0.220
		// double auxPower =powerkw*engFactor;
		//
		// //�������ϸ������ŷ�
		// emission=powerkw*timeSpan*this.mainLoadFactor()*this.mainEmFactor()+auxPower*timeSpan*this.auxLoadFactor()*this.auxEmFactor();
		// //��λ��Ҫȷ��һ�£������ǿˣ�����Ҫ��ȷ�����������
		// System.out.println("design_speed: "+ design_speed +
		// "avgSpeed: "+this.avgSpeed()+"power:"+powerkw+"sfoc:213"
		// +"emission: "+Math.round(emission));
		return Math.round(this.mainEmission() + this.auxEmission()
				+ this.boilerEmission()); // �����͸����ŷŵĺ�

	}

	// �����ŷ�
	public double mainEmission() {
		// ��Ϊ��λ
		double emission = 0;

		double timeSpan = this.spanTime / 3600;// Сʱ
		double powerkw = this.ship.getPower();
		// �������ϸ������ŷ�
		emission = powerkw * timeSpan * this.mainLoadFactor()
				* this.mainEmFactor();
		// ��λ��Ҫȷ��һ�£������ǿˣ�����Ҫ��ȷ�����������

		// System.out.println("loadfactor: "+ this.mainLoadFactor()
		// +"distance: "+ this.distance() +" time: "+ this.spanTime+
		// " avgSpeed: "+this.avgSpeed()+"power:"+powerkw+"sfoc:213"
		// +"main engine emission: "+emission);
		return Math.round(emission);

	}

	// �����ŷ�
	public double auxEmission() {
		// ��Ϊ��λ
		double emission = 0;

		double timeSpan = this.spanTime / 3600;// Сʱ
		double powerkw = this.ship.getPower();
		double engFactor = 0.220;// aux engine power/main engine power =0.220
		double auxPower = powerkw * engFactor;

		// �������ϸ������ŷ�
		emission = auxPower * timeSpan * this.auxLoadFactor()
				* this.auxEmFactor();
		// ��λ��Ҫȷ��һ�£������ǿˣ�����Ҫ��ȷ�����������

		return Math.round(emission);

	}

	public double boilerEmission() {
		// ��Ϊ��λ
		double eFactor = 922.97;

		double timeSpan = this.spanTime / 3600;// Сʱ
		int boilerEnergy = 506;
		double emission = 0;
		// ֻ����maveuring �� hotelling����״̬��Boiler�Ź�����
		if (this.speed < 8) {

			emission = boilerEnergy * timeSpan * eFactor;

		}

		return Math.round(emission);

	}

	public double fuelConsumption() {
		// assume:type:container, oil:RO
		double fuel = 0;
		// double SFOC= 213;// ����Ϊ����HFO(RO), SFOC=213g/kw.h
		double design_speed = this.ship.getSpeed();
		double timeSpan = this.spanTime / 3600;// Сʱ
		double powerkw = this.ship.getPower();
		double engFactor = 0.220;// aux engine power/main engine power =0.220
		double auxPower = powerkw * engFactor;

		// �������ϸ������ŷ�
		fuel = powerkw * timeSpan * this.mainLoadFactor() * this.mainBSFC()
				+ auxPower * timeSpan * this.auxLoadFactor() * this.auxBSFC();
		// ��λ��Ҫȷ��һ�£������ǿˣ�����Ҫ��ȷ�����������
		System.out.println("design_speed: " + design_speed + "avgSpeed: "
				+ this.avgSpeed() + "power:" + powerkw + "sfoc:213"
				+ "fuel consumption: " + Math.round(fuel));
		return Math.round(fuel);

	}

	public String getGridIds() {
		// ����ÿ����Ԫ�ߴ�Ϊ0.1X0.1�ȣ����о���360�ȣ�γ��180�ȣ����ܹ�������ָ�Ϊ1800X3600����Ԫ��
		// ����ĵ�Ԫ��gridIdsΪ����gridId�ַ��ܵĺϳɣ����е���gridId��@�ֿ�������gridIdΪgrid ���½ǵ��
		// lat*10_lon*10_�ö��ڸ�grid�г���ռ�ı���
		double cLat = this.endPoint.lat; // clat =current lat
		double cLon = this.endPoint.lon;
		double pLat = this.startPoint.lat;
		double pLon = this.startPoint.lon;

		String gridIds = "";
		int enlarge = 100; // ��ͼ�ֱ��ʿ��Ʋ���������ֵ��100ʱ ÿ����Ԫ���СΪ0.01X0.01�ȣ������10 ��
							// 0.1X0.1

		int cLatFloor = (int) Math.floor(cLat * enlarge);
		int pLatFloor = (int) Math.floor(pLat * enlarge);
		int cLonFloor = (int) Math.floor(cLon * enlarge);
		int pLonFloor = (int) Math.floor(pLon * enlarge);

		int latSpan = (int) Math.floor(cLat * enlarge)
				- (int) Math.floor(pLat * enlarge);// ά�ȷ���缸����Ԫ��(+1 �Ŷԣ�
		int lonSpan = (int) Math.floor(cLon * enlarge)
				- (int) Math.floor(pLon * enlarge);// ���ȷ���缸����Ԫ��(+1 �Ŷԣ�
		double percent = 0;// ������grid�еı���

		// System.out.println("lonspan: " + lonSpan + "   latspan: " + latSpan
		// + "   point_lat:" + Math.floor(cLat * enlarge) + "   point_lon:"
		// + Math.floor(cLon * enlarge));

		// ��������ֱ�߷���
		if (cLon == pLon) {// �����ͬ���򣬼���ֱ�������

			if (Math.abs(latSpan) > 0) {// ����ͬһ����Ԫ����

				for (int i = 0; i <= Math.abs(latSpan); i++) {

					// ��ÿ��gridռ�İٷֱ�,����i��ʾ��i����Ԫ��i��0��ʼ����0��Ϊ��С�ĵ�Ԫ��
					if (i == 0) {
						percent = (Math.min(cLatFloor, pLatFloor) + 1 - Math
								.min(cLat, pLat) * enlarge)
								/ (Math.abs(cLat - pLat) * enlarge);
					} else if (i == Math.abs(latSpan)) {
						percent = (Math.max(cLat, pLat) * enlarge - (Math.min(
								cLatFloor, pLatFloor) + i))// Ҳ������Math.max(cLatFloor,
															// pLatFloor)
								/ (Math.abs(cLat - pLat) * enlarge);
					} else {
						percent = 1 / (Math.abs(cLat - pLat) * enlarge);
					}
					// gridIds���ַ���
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
			double[] lats = new double[Math.abs(latSpan) + 1];// ά�ȷ���絥Ԫ�����
			double[] lons = new double[Math.abs(lonSpan) + 1];// ���ȷ���
			double[] y_lats = new double[Math.abs(lonSpan) + 1];// ?
			double[] x_lons = new double[Math.abs(latSpan) + 1];// ?
			double[] point_lons = new double[Math.abs(latSpan)
					+ Math.abs(lonSpan) + 2]; // ͶӰ��ά�����ϵĵ�ĸ����������˵����������Ľ��㣬��������϶���һ��grid��

			point_lons[0] = Math.min(cLon * enlarge, pLon * enlarge);// ��0������lon�ϵ�ͶӰ��������lon����
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

				midPointLon = 0.5 * (point_lons[j] + point_lons[j + 1]);// ������������е���������gridId
				midPointLat = k * midPointLon + b;
				pcent = Math.abs((point_lons[j + 1] - point_lons[j])
						/ (point_lons[point_lons.length - 1] - point_lons[0]));

				gridIds = gridIds + "@" + (int) Math.floor(midPointLon) + "_"
						+ (int) Math.floor(midPointLat) + " " // �ÿո�gridId ��
																// emission
																// percent �ֿ�
						+ pcent;

			}

		}

		return gridIds;

	}
	
	
    

	public double mainLoadFactor() {

		double factor = 0.0;
		double load = Math.pow(this.avgSpeed() / this.ship.getSpeed(), 3);
		if (load < 0.02 && this.speed >= 1) { // ����ICF 2009�� ��Сload factor Ϊ0.02
			factor = 0.02;

		} else {
			factor = load;

		}
		// ���غ�С��20%ʱ����Ҫ����һ������ϵ��������ϵ����ICF����page 43
		if (factor < 0.2 && factor > 0.01) {
			factor = factor * ((44.1 / factor + 648.6) / (44.1 / 0.2 + 648.6));
		}

		return factor;

	}

	public double auxLoadFactor() {
		// just for container type
		double loadFactor = 0;

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
		// ����ʹ��ʹ��ȼ��ΪRO��������emission factor=677.91 g/kw.h��BSFC=213g/kw.h
		double EF = 677.91;
		return EF;

	}

	public double auxEmFactor() {
		// ����ʹ��ʹ��ȼ��ΪRO��������emission factor=722.54 g/kw.h��BSFC=227g/kw.h
		double F = 227;
		return F;

	}

	public double mainBSFC() {
		// ����ʹ��ʹ��ȼ��ΪRO��������emission factor=677.91 g/kw.h��BSFC=213g/kw.h
		double F = 213;
		double factor = Math.pow(this.avgSpeed() / this.ship.getSpeed(), 3);
		// ���غ�С��20%ʱ����Ҫ����һ������ϵ��������ϵ����ICF����page 43
		if (factor < 0.2 && factor > 0.01) {
			factor = factor
					* ((14.1205 / factor + 205.7169) / (14.1205 / 0.2 + 205.7169));
		}

		return F;

	}

	public double auxBSFC() {
		// ����ʹ��ʹ��ȼ��ΪRO��������emission factor=722.54 g/kw.h��BSFC=227g/kw.h

		double EF = 227.54;
		return EF;

	}

	// save the result to a document which could be access by R program

	public void saveToFile(BufferedWriter bw) {

		try {
            //mmsi+timestamp(s)+sog/10(nm/h)+start lat(��)+start lon + end lat +end lon+distance(meters)+avgspeed(nm/h)
			//+spantime(s)+main eng emission(g)+ aux emission(g)+bioler emission(g)+ total emission+main eng load factor(%)
			String myreadline = this.startPoint.mmsi + "@"
					+ this.startPoint.timestamp + "@"
					+ this.startPoint.sog / 10 + "@" + this.startPoint.lat
					+ "@" + this.startPoint.lon + "@" + this.endPoint.lat + "@"
					+ this.endPoint.lon + "@" + ft.format(this.distance) + "@"
					+ ft.format(this.speed) + "@" + this.spanTime + "@"
					+ ft.format(this.mainEmission()) + "@" + ft.format(this.auxEmission())
					+ "@" + ft.format(this.boilerEmission()) +"@" + ft.format(this.co2Emission()) + "@" + ft.format(this.mainLoadFactor()*100);

			bw.write(myreadline); // д���ļ�
			bw.newLine();
			//System.out.println(myreadline);// ����Ļ�����

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void gridEmsToFile(BufferedWriter bw) throws IOException {

		String gridIds = this.getGridIds();
		// System.out.println("gridids: " + gridIds);
		String[] unitEmission = gridIds.split("@");// һ���ʽΪ@123_1212
													// 0.3@123_1212 0.3
		System.out.println("*******gridIds:"+gridIds+"**************");
		String[] gridIdEms = new String[2];
		
		double percent = 0;
		String gridId = null;

		for (int i = 1; i < unitEmission.length; i++) {// i=0λ��Ϊ��

			gridIdEms = unitEmission[i].split(" ");
			gridId = gridIdEms[0];
			percent = Double.parseDouble(gridIdEms[1]);

			// mmsi+star timestamp(s)+end
			// timestamp(s)+scale(�ֱ���Ϊ100)+gridId(lon(��)*100+lat*100) +
			// main eng emission(g)+ aux emission(g)+bioler
			// emission(g)+totalEmission(g)
			String myreadline = this.startPoint.mmsi + "@"
					+ this.startPoint.timestamp + "@" + this.endPoint.timestamp
					+ "@" + gridId + "@"
					+ gridId.split("_")[0] +"@"
					+ gridId.split("_")[1]+"@"
					+ ft.format(this.mainEmission() * percent) + "@"
					+ ft.format(this.auxEmission() * percent) + "@"
					+ ft.format(this.boilerEmission() * percent) + "@"
					+ ft.format(this.co2Emission() * percent);

			bw.write(myreadline); // д���ļ�
			bw.newLine();
			System.out.println(myreadline);// ����Ļ�����

		}
	}



}
