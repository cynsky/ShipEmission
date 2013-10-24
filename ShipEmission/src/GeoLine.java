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
	static DecimalFormat ft = new DecimalFormat("###0.0");//格式化设置double 123.123将变为 123 

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
		// double R = 6378137; // 地球半径，以米为单位
		// // 将经纬度转化为弧度
		// double cLonR = this.endPoint.lon * PI / 180;
		// double cLatR = this.endPoint.lat * PI / 180;
		// double pLonR = this.startPoint.lon * PI / 180;
		// double pLatR = this.startPoint.lat * PI / 180;
		// double x = (cLonR - pLonR) * Math.cos((cLatR + pLatR) / 2);
		// double y = cLatR - pLatR;
		// double distance = Math.pow(Math.pow(x, 2) + Math.pow(y, 2), 0.5) * R/
		// 1852; // 地表两点间距离，以海里为单位
		// return distance;
		double distance = this.endPoint.distanceTo(this.startPoint) / 1852;// 单位海里，此方法计算600秒以上
		 															// 偏小
		
		return distance;
	}

	public double avgSpeed() {
		int threshhold = 600; // 设置平均速度计算方式的时间临界点为300秒
		double avg_speed = 0;
		// compute average speed between two points
		if (this.timeSpan() <= threshhold) {
			avg_speed = 0.5 * 0.1 * (this.startPoint.sog + this.endPoint.sog);
		} else {

			// 距离/小时，海里每小时为单位
			avg_speed = this.distance / this.spanTime * 3600;
		}
		return avg_speed;

	}

	// 这里需要修加入如 SFOC 213g/kw.h
	public double co2Emission() {
		// double emission=0;
		// double design_speed=this.ship.getSpeed();
		// double timeSpan=this.time/3600;//小时
		// double powerkw=this.ship.getPower();
		// double engFactor=0.220;//aux engine power/main engine power =0.220
		// double auxPower =powerkw*engFactor;
		//
		// //主机加上辅机的排放
		// emission=powerkw*timeSpan*this.mainLoadFactor()*this.mainEmFactor()+auxPower*timeSpan*this.auxLoadFactor()*this.auxEmFactor();
		// //单位需要确定一下，可能是克（这里要先确定用油情况）
		// System.out.println("design_speed: "+ design_speed +
		// "avgSpeed: "+this.avgSpeed()+"power:"+powerkw+"sfoc:213"
		// +"emission: "+Math.round(emission));
		return Math.round(this.mainEmission() + this.auxEmission()
				+ this.boilerEmission()); // 主机和辅机排放的和

	}

	// 主机排放
	public double mainEmission() {
		// 克为单位
		double emission = 0;

		double timeSpan = this.spanTime / 3600;// 小时
		double powerkw = this.ship.getPower();
		// 主机加上辅机的排放
		emission = powerkw * timeSpan * this.mainLoadFactor()
				* this.mainEmFactor();
		// 单位需要确定一下，可能是克（这里要先确定用油情况）

		// System.out.println("loadfactor: "+ this.mainLoadFactor()
		// +"distance: "+ this.distance() +" time: "+ this.spanTime+
		// " avgSpeed: "+this.avgSpeed()+"power:"+powerkw+"sfoc:213"
		// +"main engine emission: "+emission);
		return Math.round(emission);

	}

	// 辅机排放
	public double auxEmission() {
		// 克为单位
		double emission = 0;

		double timeSpan = this.spanTime / 3600;// 小时
		double powerkw = this.ship.getPower();
		double engFactor = 0.220;// aux engine power/main engine power =0.220
		double auxPower = powerkw * engFactor;

		// 主机加上辅机的排放
		emission = auxPower * timeSpan * this.auxLoadFactor()
				* this.auxEmFactor();
		// 单位需要确定一下，可能是克（这里要先确定用油情况）

		return Math.round(emission);

	}

	public double boilerEmission() {
		// 克为单位
		double eFactor = 922.97;

		double timeSpan = this.spanTime / 3600;// 小时
		int boilerEnergy = 506;
		double emission = 0;
		// 只有在maveuring 和 hotelling两种状态下Boiler才工作。
		if (this.speed < 8) {

			emission = boilerEnergy * timeSpan * eFactor;

		}

		return Math.round(emission);

	}

	public double fuelConsumption() {
		// assume:type:container, oil:RO
		double fuel = 0;
		// double SFOC= 213;// 假设为重油HFO(RO), SFOC=213g/kw.h
		double design_speed = this.ship.getSpeed();
		double timeSpan = this.spanTime / 3600;// 小时
		double powerkw = this.ship.getPower();
		double engFactor = 0.220;// aux engine power/main engine power =0.220
		double auxPower = powerkw * engFactor;

		// 主机加上辅机的排放
		fuel = powerkw * timeSpan * this.mainLoadFactor() * this.mainBSFC()
				+ auxPower * timeSpan * this.auxLoadFactor() * this.auxBSFC();
		// 单位需要确定一下，可能是克（这里要先确定用油情况）
		System.out.println("design_speed: " + design_speed + "avgSpeed: "
				+ this.avgSpeed() + "power:" + powerkw + "sfoc:213"
				+ "fuel consumption: " + Math.round(fuel));
		return Math.round(fuel);

	}

	public String getGridIds() {
		// 假设每个单元尺寸为0.1X0.1度，其中经度360度，纬度180度，故总共将地球分割为1800X3600个单元。
		// 这里的单元格gridIds为单个gridId字符窜的合成，其中单个gridId由@分开。单个gridId为grid 左下角点的
		// lat*10_lon*10_该段在该grid中长度占的比例
		double cLat = this.endPoint.lat; // clat =current lat
		double cLon = this.endPoint.lon;
		double pLat = this.startPoint.lat;
		double pLon = this.startPoint.lon;

		String gridIds = "";
		int enlarge = 100; // 地图分辨率控制参数，当其值曲100时 每个单元格大小为0.01X0.01度，如果是10 则
							// 0.1X0.1

		int cLatFloor = (int) Math.floor(cLat * enlarge);
		int pLatFloor = (int) Math.floor(pLat * enlarge);
		int cLonFloor = (int) Math.floor(cLon * enlarge);
		int pLonFloor = (int) Math.floor(pLon * enlarge);

		int latSpan = (int) Math.floor(cLat * enlarge)
				- (int) Math.floor(pLat * enlarge);// 维度方向跨几个单元格(+1 才对）
		int lonSpan = (int) Math.floor(cLon * enlarge)
				- (int) Math.floor(pLon * enlarge);// 经度方向跨几个单元格(+1 才对）
		double percent = 0;// 航段在grid中的比例

		// System.out.println("lonspan: " + lonSpan + "   latspan: " + latSpan
		// + "   point_lat:" + Math.floor(cLat * enlarge) + "   point_lon:"
		// + Math.floor(cLon * enlarge));

		// 用两点求直线方程
		if (cLon == pLon) {// 与进度同方向，即垂直赤道方向

			if (Math.abs(latSpan) > 0) {// 不在同一个单元格内

				for (int i = 0; i <= Math.abs(latSpan); i++) {

					// 求每个grid占的百分比,其中i表示第i个单元格，i从0开始，第0个为最小的单元格
					if (i == 0) {
						percent = (Math.min(cLatFloor, pLatFloor) + 1 - Math
								.min(cLat, pLat) * enlarge)
								/ (Math.abs(cLat - pLat) * enlarge);
					} else if (i == Math.abs(latSpan)) {
						percent = (Math.max(cLat, pLat) * enlarge - (Math.min(
								cLatFloor, pLatFloor) + i))// 也可以是Math.max(cLatFloor,
															// pLatFloor)
								/ (Math.abs(cLat - pLat) * enlarge);
					} else {
						percent = 1 / (Math.abs(cLat - pLat) * enlarge);
					}
					// gridIds的字符窜
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
			double[] lats = new double[Math.abs(latSpan) + 1];// 维度方向跨单元格个数
			double[] lons = new double[Math.abs(lonSpan) + 1];// 经度方向
			double[] y_lats = new double[Math.abs(lonSpan) + 1];// ?
			double[] x_lons = new double[Math.abs(latSpan) + 1];// ?
			double[] point_lons = new double[Math.abs(latSpan)
					+ Math.abs(lonSpan) + 2]; // 投影到维度轴上的点的个数，两个端点加上与网格的交点，相邻两点肯定在一个grid中

			point_lons[0] = Math.min(cLon * enlarge, pLon * enlarge);// 第0个点在lon上的投影，或者是lon坐标
			point_lons[Math.abs(latSpan) + Math.abs(lonSpan) + 1] = Math.max(
					cLon * enlarge, pLon * enlarge);

			// get the lat direct crosses
			if (Math.abs(latSpan) > 0) {
				for (int j = 1; j <= Math.abs(latSpan); j++) {
					lats[j] = Math.min(cLatFloor, pLatFloor) + j;
					x_lons[j] = (lats[j] - b) / k;
					point_lons[j] = x_lons[j];// 第j个点lon坐标

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

				midPointLon = 0.5 * (point_lons[j] + point_lons[j + 1]);// 相邻两个点的中点用来计算gridId
				midPointLat = k * midPointLon + b;
				pcent = Math.abs((point_lons[j + 1] - point_lons[j])
						/ (point_lons[point_lons.length - 1] - point_lons[0]));

				gridIds = gridIds + "@" + (int) Math.floor(midPointLon) + "_"
						+ (int) Math.floor(midPointLat) + " " // 用空格将gridId 与
																// emission
																// percent 分开
						+ pcent;

			}

		}

		return gridIds;

	}
	
	
    

	public double mainLoadFactor() {

		double factor = 0.0;
		double load = Math.pow(this.avgSpeed() / this.ship.getSpeed(), 3);
		if (load < 0.02 && this.speed >= 1) { // 根据ICF 2009， 最小load factor 为0.02
			factor = 0.02;

		} else {
			factor = load;

		}
		// 当载荷小于20%时，需要乘以一个调整系数，调整系数见ICF文章page 43
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
		// 假设使用使用燃油为RO，则主机emission factor=677.91 g/kw.h，BSFC=213g/kw.h
		double EF = 677.91;
		return EF;

	}

	public double auxEmFactor() {
		// 假设使用使用燃油为RO，则主机emission factor=722.54 g/kw.h，BSFC=227g/kw.h
		double F = 227;
		return F;

	}

	public double mainBSFC() {
		// 假设使用使用燃油为RO，则主机emission factor=677.91 g/kw.h，BSFC=213g/kw.h
		double F = 213;
		double factor = Math.pow(this.avgSpeed() / this.ship.getSpeed(), 3);
		// 当载荷小于20%时，需要乘以一个调整系数，调整系数见ICF文章page 43
		if (factor < 0.2 && factor > 0.01) {
			factor = factor
					* ((14.1205 / factor + 205.7169) / (14.1205 / 0.2 + 205.7169));
		}

		return F;

	}

	public double auxBSFC() {
		// 假设使用使用燃油为RO，则主机emission factor=722.54 g/kw.h，BSFC=227g/kw.h

		double EF = 227.54;
		return EF;

	}

	// save the result to a document which could be access by R program

	public void saveToFile(BufferedWriter bw) {

		try {
            //mmsi+timestamp(s)+sog/10(nm/h)+start lat(度)+start lon + end lat +end lon+distance(meters)+avgspeed(nm/h)
			//+spantime(s)+main eng emission(g)+ aux emission(g)+bioler emission(g)+ total emission+main eng load factor(%)
			String myreadline = this.startPoint.mmsi + "@"
					+ this.startPoint.timestamp + "@"
					+ this.startPoint.sog / 10 + "@" + this.startPoint.lat
					+ "@" + this.startPoint.lon + "@" + this.endPoint.lat + "@"
					+ this.endPoint.lon + "@" + ft.format(this.distance) + "@"
					+ ft.format(this.speed) + "@" + this.spanTime + "@"
					+ ft.format(this.mainEmission()) + "@" + ft.format(this.auxEmission())
					+ "@" + ft.format(this.boilerEmission()) +"@" + ft.format(this.co2Emission()) + "@" + ft.format(this.mainLoadFactor()*100);

			bw.write(myreadline); // 写入文件
			bw.newLine();
			//System.out.println(myreadline);// 在屏幕上输出

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void gridEmsToFile(BufferedWriter bw) throws IOException {

		String gridIds = this.getGridIds();
		// System.out.println("gridids: " + gridIds);
		String[] unitEmission = gridIds.split("@");// 一般格式为@123_1212
													// 0.3@123_1212 0.3
		System.out.println("*******gridIds:"+gridIds+"**************");
		String[] gridIdEms = new String[2];
		
		double percent = 0;
		String gridId = null;

		for (int i = 1; i < unitEmission.length; i++) {// i=0位置为空

			gridIdEms = unitEmission[i].split(" ");
			gridId = gridIdEms[0];
			percent = Double.parseDouble(gridIdEms[1]);

			// mmsi+star timestamp(s)+end
			// timestamp(s)+scale(分辨率为100)+gridId(lon(度)*100+lat*100) +
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

			bw.write(myreadline); // 写入文件
			bw.newLine();
			System.out.println(myreadline);// 在屏幕上输出

		}
	}



}
