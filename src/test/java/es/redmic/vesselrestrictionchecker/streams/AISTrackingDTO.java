package es.redmic.vesselrestrictionchecker.streams;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class AISTrackingDTO extends org.apache.avro.specific.SpecificRecordBase
		implements org.apache.avro.specific.SpecificRecord {

	//@formatter:off

	public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
		"{\"type\":\"record\",\"name\":\"AISTrackingDTO\",\"namespace\":"
			+ "\"es.redmic.vesselrestrictionchecker.streams\",\"fields\":["
		+ "{\"name\":\"mmsi\",\"type\":\"int\"},"
		+ "{\"name\":\"tstamp\",\"type\":{ \"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
		+ "{\"name\":\"latitude\",\"type\":\"double\"},"
		+ "{\"name\":\"longitude\",\"type\":\"double\"},"
		+ "{\"name\":\"type\",\"type\":\"int\"},"
		+ "{\"name\":\"name\",\"type\":\"string\"}]}");
	//@formatter:on

	private Integer mmsi;

	private DateTime tstamp;

	private Double latitude;

	private Double longitude;

	private Integer type = 0;

	private String name;

	public AISTrackingDTO() {
	}

	public Integer getMmsi() {
		return mmsi;
	}

	public void setMmsi(Integer mmsi) {
		this.mmsi = mmsi;
	}

	public DateTime getTstamp() {
		return tstamp;
	}

	public void setTstamp(DateTime tstamp) {
		this.tstamp = tstamp;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {

		if (type == null)
			this.type = 0;
		else
			this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public org.apache.avro.Schema getSchema() {
		return SCHEMA$;
	}

	// Used by DatumWriter. Applications should not call.
	@Override
	public java.lang.Object get(int field$) {
		switch (field$) {
		case 0:
			return mmsi;
		case 1:
			return tstamp.getMillis();
		case 2:
			return latitude;
		case 3:
			return longitude;
		case 4:
			return type;
		case 5:
			return name;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	// Used by DatumReader. Applications should not call.
	@Override
	public void put(int field$, java.lang.Object value$) {
		switch (field$) {
		case 0:
			mmsi = (java.lang.Integer) value$;
			break;
		case 1:
			tstamp = new DateTime(value$, DateTimeZone.UTC);
			break;
		case 2:
			latitude = (java.lang.Double) value$;
			break;
		case 3:
			longitude = (java.lang.Double) value$;
			break;
		case 4:
			type = (java.lang.Integer) value$;
			break;
		case 5:
			name = value$ != null ? value$.toString() : null;
			break;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}
}
