
/*
 * Copyright 2010, 2011, 2012 mapsforge.org
 * Copyright 2012 osmdroid authors: Nicolas Gramlich, Theodore Hong
 * Copyright 2012 Hannes Janetzek
 *
 * This program is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */


/**
 * A GeoPoint represents an immutable pair of latitude and longitude
 * coordinates.
 */
public class GeoPoint implements Cloneable{
	/**
	 * Conversion factor from degrees to microdegrees.
	 */
	private static final double CONVERSION_FACTOR = 1000000d;

	/**
	 * The latitude value of this GeoPoint in microdegrees (degrees * 10^6).
	 */
	public final int latitudeE6;

	/**
	 * The longitude value of this GeoPoint in microdegrees (degrees * 10^6).
	 */
	public final int longitudeE6;
	public final String mmsi;
	public final long timestamp;
	public final double sog;
	public final double lon;
	public final double lat;
	public final double cog;
	
public GeoPoint(String mmsi,long timestamp,double sog,double lat,double lon,double cog) {
	    this.mmsi=mmsi;
	    this.timestamp=timestamp;
	    this.sog=sog;
	    this.lon=lon;
	    this.lat=lat;
	    this.cog=cog;
		this.latitudeE6 = (int) (lat * CONVERSION_FACTOR);	
		this.longitudeE6 = (int) (lon * CONVERSION_FACTOR);
	}

	
	/**
	 * @param latitude
	 *            the latitude in degrees, will be limited to the possible
	 *            latitude range.
	 * @param longitude
	 *            the longitude in degrees, will be limited to the possible
	 *            longitude range.
	 */
	
//	public GeoPoint(double latitude, double longitude) {
//		
//		this.latitudeE6 = (int) (latitude * CONVERSION_FACTOR);
//
//		
//		this.longitudeE6 = (int) (longitude * CONVERSION_FACTOR);
//	}

	/**
	 * @param latitudeE6
	 *            the latitude in microdegrees (degrees * 10^6), will be limited
	 *            to the possible latitude range.
	 * @param longitudeE6
	 *            the longitude in microdegrees (degrees * 10^6), will be
	 *            limited to the possible longitude range.
	 */
//	public GeoPoint(int latitudeE6, int longitudeE6 ) {
//		this(latitudeE6 / CONVERSION_FACTOR, longitudeE6 / CONVERSION_FACTOR);
//	}


     @Override
     public Object clone(){
    	 GeoPoint pnt=null;
    	 try {
			pnt=(GeoPoint)super.clone();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	 
    	 return pnt;
     }
     


	
	/**
	 * @return the latitude value of this GeoPoint in degrees.
	 */
	public double getLatitude() {
		return this.latitudeE6 / CONVERSION_FACTOR;
	}
	
	

	/**
	 * @return the longitude value of this GeoPoint in degrees.
	 */
	public double getLongitude() {
		return this.longitudeE6 / CONVERSION_FACTOR;
	}
	

	



	// ===========================================================
	// Methods from osmdroid
	// ===========================================================

	public static final float DEG2RAD = (float) (Math.PI / 180.0);
	public static final float RAD2DEG = (float) (180.0 / Math.PI);
	// http://en.wikipedia.org/wiki/Earth_radius#Equatorial_radius
	public static final int RADIUS_EARTH_METERS = 6378137;

	/**
	 * @see "http://www.geocities.com/DrChengalva/GPSDistance.html"
	 * @param other
	 *            ...
	 * @return distance in meters
	 */
	public double distanceTo(final GeoPoint other) {

		double a1 = DEG2RAD * latitudeE6/1E6;
		double a2 = DEG2RAD * longitudeE6/1E6;
		double b1 = DEG2RAD * other.latitudeE6/1E6;
		double b2 = DEG2RAD * other.longitudeE6/1E6;

		double cosa1 = Math.cos(a1);
		double cosb1 = Math.cos(b1);

		double t1 = cosa1 * Math.cos(a2) * cosb1 * Math.cos(b2);
		double t2 = cosa1 * Math.sin(a2) * cosb1 * Math.sin(b2);

		double t3 = Math.sin(a1) * Math.sin(b1);

		double tt = Math.acos(t1 + t2 + t3);

		//return (int) (RADIUS_EARTH_METERS * tt);
		//������double
		return RADIUS_EARTH_METERS * tt;
	}

	
	
}