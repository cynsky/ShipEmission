import java.util.ArrayList;
import java.util.List;
import java.lang.Math;

public class AISTrajectoryCompress {
	
	public static List<GeoPoint> reduceWithTolerance(List<GeoPoint> shape,
			double tolerance) {
		int n = shape.size();
		// if a shape has 2 or less points it cannot be reduced
		if (tolerance <= 0 || n < 3) {
			return shape;
		}

		boolean[] marked = new boolean[n]; // vertex indexes to keep will be
											// marked as "true"
		for (int i = 1; i < n - 1; i++)
			marked[i] = false;
		// automatically add the first point to the returned shape
		marked[0] = true;

		trajectoryReduction(shape, // original shape
				marked, // reduced shape
				tolerance // tolerance
		);

		// all done, return the reduced shape
		List<GeoPoint> newShape = new ArrayList<GeoPoint>(n); // the new
																	// shape to															// // return
		for (int i = 0; i < n; i++) {
			if (marked[i])
				newShape.add(shape.get(i));
		}
		return newShape;
	}

	private static void trajectoryReduction(List<GeoPoint> shape,
			boolean[] marked, double tolerance) {

		GeoPoint firstPoint;
		GeoPoint secondPoint;
		double distance = 0;
		int length = shape.size();
		firstPoint = shape.get(0);
		for (int i = 1; i < length - 1; i++) {
			secondPoint = shape.get(i);
//			System.out.println("first speed lat lon : "+firstPoint.sog +" "+firstPoint.latitudeE6 + " "+firstPoint.longitudeE6
//					+"second : "+secondPoint.sog +" "+secondPoint.latitudeE6 + " "+secondPoint.longitudeE6 );
			distance = twoPointDistance(firstPoint, secondPoint);
			if (distance > tolerance) {
				marked[i] = true;
				firstPoint = shape.get(i);
			}
		}

	}

	public static double twoPointDistance(GeoPoint firstPoint,
			GeoPoint secondPoint) {
		double speedDistance = Math.abs(secondPoint.sog - firstPoint.sog);
		int latDistance = Math.abs(secondPoint.latitudeE6
				- firstPoint.latitudeE6);
		int lonDistance = Math.abs(secondPoint.longitudeE6
				- firstPoint.longitudeE6);
		return Math.max(speedDistance, Math.max(latDistance, lonDistance));

	}
}
