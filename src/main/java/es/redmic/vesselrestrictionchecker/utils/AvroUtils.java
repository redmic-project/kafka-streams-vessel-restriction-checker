package es.redmic.vesselrestrictionchecker.utils;

import java.lang.reflect.Type;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecord;

public class AvroUtils {

	public static Object getSpecificRecordProperty(SpecificRecord record, String name) {

		return record.get(record.getSchema().getField(name).pos());
	}

	public static HashMap<String, GenericRecord> aggregateGenericRecordInMap(String k, GenericRecord v,
			HashMap<String, GenericRecord> map) {
		map.put(k, v);
		return map;
	}

	public static GenericRecord getGenericRecordFromClass(Type classs) {

		Schema schema = ReflectData.get().getSchema(classs);
		return new GenericData.Record(schema);
	}
}
