package es.redmic.vesselrestrictionchecker.dto;

public class SimplePoint {

	private String mmsi;

	private String geometry;

	private String geohash;

	private String name;

	private long dateTime;

	private Integer vesselType;

	private Double sog;

	public String getMmsi() {
		return mmsi;
	}

	public void setMmsi(String mmsi) {
		this.mmsi = mmsi;
	}

	public String getGeometry() {
		return geometry;
	}

	public void setGeometry(String geometry) {
		this.geometry = geometry;
	}

	public String getGeohash() {
		return geohash;
	}

	public void setGeohash(String geohash) {
		this.geohash = geohash;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getDateTime() {
		return dateTime;
	}

	public void setDateTime(long dateTime) {
		this.dateTime = dateTime;
	}

	public Integer getVesselType() {
		return vesselType;
	}

	public void setVesselType(Integer vesselType) {
		this.vesselType = vesselType;
	}

	public Double getSog() {
		return sog;
	}

	public void setSog(Double sog) {
		this.sog = sog;
	}
}
