package es.redmic.vesselrestrictionchecker.streams;

import org.apache.avro.Schema;

public class AreaDTO extends org.apache.avro.specific.SpecificRecordBase
		implements org.apache.avro.specific.SpecificRecord {

	//@formatter:off

	public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
		"{\"type\":\"record\",\"name\":\"AreaDTO\",\"namespace\":"
			+ "\"es.redmic.vesselrestrictionchecker.streams\",\"fields\":["
		+ "{\"name\":\"id\",\"type\":\"string\"},"
		+ "{\"name\":\"name\",\"type\":\"string\"},"
		+ "{\"name\":\"geometry\",\"type\":\"string\"}]}");
	//@formatter:on

	private String id;

	private String name;

	private String geometry;

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
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

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
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}
}
