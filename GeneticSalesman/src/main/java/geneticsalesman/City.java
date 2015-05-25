package geneticsalesman;

import java.io.Serializable;

public class City implements Serializable {

	private static final int EARTH_RADIUS=6378388; //meters
	
	private final String name;
	private final double longitude;
	private final double latitude;
	private int id;

	public City(int id, String name, double longitude, double latitude) {
		this.name=name;
		this.longitude=longitude;
		this.latitude=latitude;
		this.id=id;
	}

	public String getName() {
		return name;
	}

	public double getLongitude() {
		return longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public double distanceTo(City otherCity) {
	    /*double dLat = Math.toRadians(otherCity.latitude-this.latitude);
	    double dLng = Math.toRadians(otherCity.longitude-this.longitude);
	    double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
	               Math.cos(Math.toRadians(this.latitude)) * Math.cos(Math.toRadians(otherCity.latitude)) *
	               Math.sin(dLng/2) * Math.sin(dLng/2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	    return EARTH_RADIUS * c;*/
		double q1=Math.cos(Math.toRadians(otherCity.longitude-this.longitude));
		double q2=Math.cos(Math.toRadians(otherCity.latitude-this.latitude));
		double q3=Math.cos(Math.toRadians(otherCity.latitude+this.latitude));
		double distance=EARTH_RADIUS*Math.acos(0.5*((1.0+q1)*q2-(1.0-q1)*q3))+1.0;
		return distance;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		City other = (City) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "City [id="+id+", name=" + name + ", longitude=" + longitude
				+ ", latitude=" + latitude + "]";
	}

	public int getId() {
		return id;
	}

}
