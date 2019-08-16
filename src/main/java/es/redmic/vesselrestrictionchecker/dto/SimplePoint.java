package es.redmic.vesselrestrictionchecker.dto;

import org.joda.time.DateTime;

public class SimplePoint {

	private String mmsi;

	private String geometry;

	private String geohash;

	private String name;

	private DateTime dateTime;

	private Integer vesselType;

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

	public DateTime getDateTime() {
		return dateTime;
	}

	public void setDateTime(DateTime dateTime) {
		this.dateTime = dateTime;
	}

	public Integer getVesselType() {
		return vesselType;
	}

	public void setVesselType(Integer vesselType) {
		this.vesselType = vesselType;
	}
}
