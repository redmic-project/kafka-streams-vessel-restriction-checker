package es.redmic.vesselrestrictionchecker.streams;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class AreaDTO extends org.apache.avro.specific.SpecificRecordBase
		implements org.apache.avro.specific.SpecificRecord {

	//@formatter:off

	public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
		"{\"type\":\"record\",\"name\":\"AreaDTO\",\"namespace\":"
			+ "\"es.redmic.vesselrestrictionchecker.streams\",\"fields\":["
		+ "{\"name\":\"id\",\"type\":\"string\"},"
		+ "{\"name\":\"name\",\"type\":\"string\"},"
		+ "{\"name\":\"geometry\",\"type\":\"string\"},"
		+ "{\"name\":\"vesselTypesRestricted\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]},"
		+ "{\"name\":\"maxSpeed\",\"type\":[\"null\", \"double\"]}]}");
	//@formatter:on

	private String id;

	private String name;

	private String geometry;

	private List<String> vesselTypesRestricted;

	private Double maxSpeed;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getGeometry() {
		return geometry;
	}

	public void setGeometry(String geometry) {
		this.geometry = geometry;
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

	@Override
	public Schema getSchema() {
		return SCHEMA$;
	}

	@Override
	public Object get(int field) {
		switch (field) {
		case 0:
			return id;
		case 1:
			return name;
		case 2:
			return geometry;
		case 3:
			return vesselTypesRestricted;
		case 4:
			return maxSpeed;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void put(int field, Object value) {
		switch (field) {
		case 0:
			id = value.toString();
			break;
		case 1:
			name = value.toString();
			break;
		case 2:
			geometry = value.toString();
			break;
		case 3:
			vesselTypesRestricted = value != null ? getStringList((java.util.List) value) : null;
			break;
		case 4:
			maxSpeed = value != null ? (Double) value : null;
			break;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	@JsonIgnore
	private List<String> getStringList(List<?> value) {

		return value.stream().map(s -> s.toString()).collect(Collectors.toList());
	}
}
