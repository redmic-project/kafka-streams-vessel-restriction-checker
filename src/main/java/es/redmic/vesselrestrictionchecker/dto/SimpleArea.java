package es.redmic.vesselrestrictionchecker.dto;

import java.util.List;

import org.apache.avro.reflect.Nullable;

public class SimpleArea {

	private String id;

	private String geometry;

	private String geohash;

	private String name;

	@Nullable
	private List<String> vesselTypesRestricted;

	@Nullable
	private Double maxSpeed;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public List<String> getVesselTypesRestricted() {
		return vesselTypesRestricted;
	}

	public void setVesselTypesRestricted(List<String> vesselTypesRestricted) {
		this.vesselTypesRestricted = vesselTypesRestricted;
	}

	public Double getMaxSpeed() {
		return maxSpeed;
	}

	public void setMaxSpeed(Double maxSpeed) {
		this.maxSpeed = maxSpeed;
	}
}
