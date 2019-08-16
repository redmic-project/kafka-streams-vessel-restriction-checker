package es.redmic.vesselrestrictionchecker.dto;

import org.apache.avro.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class PointInAreaAlert extends org.apache.avro.specific.SpecificRecordBase
		implements org.apache.avro.specific.SpecificRecord {

	//@formatter:off

	public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
		"{\"type\":\"record\",\"name\":\"PointInAreaAlert\",\"namespace\":"
			+ "\"es.redmic.vesselrestrictionchecker.dto\",\"fields\":["
		+ "{\"name\":\"areaId\",\"type\":\"string\"},"
		+ "{\"name\":\"areaName\",\"type\":\"string\"},"
		+ "{\"name\":\"vesselMmsi\",\"type\":\"string\"},"
		+ "{\"name\":\"vesselName\",\"type\":\"string\"},"
		+ "{\"name\":\"geometry\",\"type\":\"string\"},"
		+ "{\"name\":\"dateTime\",\"type\":{ \"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
		+ "{\"name\":\"vesselType\",\"type\":\"int\"}]}");
	//@formatter:on

	private String areaId;

	private String areaName;

	private String vesselMmsi;

	private String vesselName;

	private String geometry;

	private DateTime dateTime;

	private Integer vesselType;

	public String getAreaId() {
		return areaId;
	}

	public void setAreaId(String areaId) {
		this.areaId = areaId;
	}

	public String getAreaName() {
		return areaName;
	}

	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}

	public String getVesselMmsi() {
		return vesselMmsi;
	}

	public void setVesselMmsi(String vesselMmsi) {
		this.vesselMmsi = vesselMmsi;
	}

	public String getVesselName() {
		return vesselName;
	}

	public void setVesselName(String vesselName) {
		this.vesselName = vesselName;
	}

	public String getGeometry() {
		return geometry;
	}

	public void setGeometry(String geometry) {
		this.geometry = geometry;
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

	@Override
	public Schema getSchema() {
		return SCHEMA$;
	}

	@Override
	public Object get(int field) {
		switch (field) {
		case 0:
			return areaId;
		case 1:
			return areaName;
		case 2:
			return vesselMmsi;
		case 3:
			return vesselName;
		case 4:
			return geometry;
		case 5:
			return dateTime.getMillis();
		case 6:
			return vesselType;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	@Override
	public void put(int field, Object value) {
		switch (field) {
		case 0:
			areaId = value.toString();
			break;
		case 1:
			areaName = value.toString();
			break;
		case 2:
			vesselMmsi = value.toString();
			break;
		case 3:
			vesselName = value.toString();
			break;
		case 4:
			geometry = value.toString();
			break;
		case 5:
			dateTime = new DateTime(value, DateTimeZone.UTC);
			break;
		case 6:
			vesselType = (java.lang.Integer) value;
			break;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}
}
