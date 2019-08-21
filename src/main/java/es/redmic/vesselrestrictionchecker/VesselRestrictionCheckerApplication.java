package es.redmic.vesselrestrictionchecker;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

import es.redmic.vesselrestrictionchecker.avro.hashmapserde.HashMapSerde;
import es.redmic.vesselrestrictionchecker.common.StreamsApplicationBase;
import es.redmic.vesselrestrictionchecker.dto.PointInAreaAlert;
import es.redmic.vesselrestrictionchecker.dto.SimpleArea;
import es.redmic.vesselrestrictionchecker.dto.SimplePoint;
import es.redmic.vesselrestrictionchecker.utils.AvroUtils;
import es.redmic.vesselrestrictionchecker.utils.GeoUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class VesselRestrictionCheckerApplication extends StreamsApplicationBase {

	// @formatter:off
	
	private static final String AREAS_TOPIC = "AREAS_TOPIC",
			POINTS_TOPIC = "POINTS_TOPIC",
			RESULT_TOPIC = "RESULT_TOPIC";

	private static final int GEO_HASH_PRECISION = 3;

	private static final String GEO_HASH_KEY = "geohash",
			RESULT_GEOMETRY_PROPERTY = "geometry",
			RESULT_VESSEL_MMSI_PROPERTY = "vesselMmsi";
	
	// @formatter:on

	@SuppressWarnings("serial")
	private static HashMap<String, String> requiredVariables = new HashMap<String, String>() {
		{
			putAll(requiredVariablesBase);
			put(AREAS_TOPIC, "Kafka topic for receiving geofencing areas");
			put(POINTS_TOPIC, "Kafka topic for receiving points to check");
			put(RESULT_TOPIC, "Kafka topic for sending checking result");
		}
	};

	public VesselRestrictionCheckerApplication(SchemaRegistryClient schemaRegistryClient, String schemaRegistryUrl) {
		super(schemaRegistryClient, schemaRegistryUrl);
	}

	public Topology getTopology(String pointsTopic, String areasTopic, String resultTopic) {

		StreamsBuilder builder = new StreamsBuilder();

		HashMapSerde<String, GenericRecord> hashMapSerde = new HashMapSerde<String, GenericRecord>(schemaRegistryClient,
				schemaRegistryUrl);

		KStream<String, SpecificRecord> areasStream = builder.stream(areasTopic,
				Consumed.with(null, getSpecificAvroSerde()));

		KStream<String, SpecificRecord> pointsStream = builder.stream(pointsTopic,
				Consumed.with(null, getSpecificAvroSerde()));

		KStream<String, SpecificRecord> lastAreasKStream = areasStream
				.groupByKey(Serialized.with(null, getSpecificAvroSerde()))
				.reduce((aggValue, newValue) -> newValue, Materialized.with(null, getSpecificAvroSerde())).toStream();

		KStream<String, GenericRecord> areasKStreamEnriched = lastAreasKStream
				.flatMapValues((value) -> enrichAreaWithGeoHash(value)).selectKey((key, value) -> getGeoHashKey(value));

		KTable<String, HashMap<String, GenericRecord>> areasKTableAgg = areasKStreamEnriched
				.groupByKey(Serialized.with(null, getGenericAvroSerde())).aggregate(HashMap<String, GenericRecord>::new,
						// Agrega las diferentes áreas con el mismo geoHashCode
						(k, v, map) -> AvroUtils.aggregateGenericRecordInMap(k, v, map, "id"),
						Materialized.with(null, hashMapSerde));

		KStream<String, GenericRecord> pointsStreamEnriched = pointsStream
				.mapValues(value -> enrichPointWithGeoHash(value)).selectKey((k, v) -> getGeoHashKey(v));

		pointsStreamEnriched
				.join(areasKTableAgg, (point, areas) -> getPointInAreaAlert(point, areas),
						Joined.valueSerde(getGenericAvroSerde()))
				.flatMapValues(value -> value).selectKey((k, v) -> v.get(RESULT_VESSEL_MMSI_PROPERTY))
				.to(resultTopic, Produced.with(null, getSpecificAvroSerde()));

		return builder.build();
	}

	private String getGeoHashKey(GenericRecord v) {
		return v.get(GEO_HASH_KEY).toString();
	}

	private GenericRecord enrichPointWithGeoHash(SpecificRecord value) {

		GenericRecord avroRecord = AvroUtils.getGenericRecordFromClass(SimplePoint.class);

		String geometry;

		try {
			geometry = GeoUtils.getWKTGeometry(value);
			avroRecord.put(RESULT_GEOMETRY_PROPERTY, geometry);

			List<String> geoHashList = GeoUtils.getGeoHash(geometry, GEO_HASH_PRECISION);

			if (geoHashList == null | geoHashList.size() == 0)
				return null;
			// Como se trata de un punto solo devolverá un elemento
			avroRecord.put(GEO_HASH_KEY, geoHashList.get(0));
		} catch (InvalidShapeException | ParseException e) {
			e.printStackTrace();
			return null;
		}
		// Se crea un nuevo registro con el geohash code y solo con la info necesaria
		avroRecord.put("mmsi", AvroUtils.getSpecificRecordProperty(value, "mmsi").toString());
		avroRecord.put("name", AvroUtils.getSpecificRecordProperty(value, "name").toString());
		avroRecord.put("dateTime", Long.parseLong(AvroUtils.getSpecificRecordProperty(value, "tstamp").toString()));
		avroRecord.put("vesselType", Integer.parseInt(AvroUtils.getSpecificRecordProperty(value, "type").toString()));

		return avroRecord;
	}

	private List<GenericRecord> enrichAreaWithGeoHash(SpecificRecord value) {

		String geometry;
		try {
			geometry = GeoUtils.getWKTGeometry(value);
		} catch (InvalidShapeException | ParseException e1) {
			e1.printStackTrace();
			return null;
		}

		if (geometry != null) {

			List<GenericRecord> values = new ArrayList<>();
			try {
				List<String> geoHashList = GeoUtils.getGeoHash(geometry, GEO_HASH_PRECISION);

				for (String geoHash : geoHashList) {
					// Se crean nuevos registros con el geohash code y solo con la info necesaria
					GenericRecord avroRecord = AvroUtils.getGenericRecordFromClass(SimpleArea.class);
					avroRecord.put(RESULT_GEOMETRY_PROPERTY, geometry.toString());
					avroRecord.put("id", AvroUtils.getSpecificRecordProperty(value, "id").toString());
					avroRecord.put("name", AvroUtils.getSpecificRecordProperty(value, "name").toString());
					avroRecord.put(GEO_HASH_KEY, geoHash);
					values.add(avroRecord);
				}
			} catch (InvalidShapeException | ParseException e) {
				e.printStackTrace();
			}
			return values;
		}
		return null;
	}

	private ArrayList<PointInAreaAlert> getPointInAreaAlert(GenericRecord pointRecord,
			HashMap<String, GenericRecord> areas) {

		ArrayList<PointInAreaAlert> result = new ArrayList<>();

		String point_wkt = pointRecord.get(RESULT_GEOMETRY_PROPERTY).toString();

		Shape point;

		try {
			point = GeoUtils.getShapeFromWKT(point_wkt);
		} catch (InvalidShapeException | ParseException e1) {
			e1.printStackTrace();
			return result;
		}

		for (Map.Entry<String, GenericRecord> entry : areas.entrySet()) {

			GenericRecord areaRecord = entry.getValue();

			String geometry_wkt = areaRecord.get(RESULT_GEOMETRY_PROPERTY).toString();

			Shape area;

			try {
				area = GeoUtils.getShapeFromWKT(geometry_wkt);
			} catch (InvalidShapeException | ParseException e) {
				e.printStackTrace();
				break;
			}

			// TODO: analizar si es necesario seguir procesando elementos una vez encontrada
			// un área.
			// Al menos no seguir procesando elementos de la misma área
			if (area.relate(point) == SpatialRelation.CONTAINS) {

				// Se crea una alerta con la info básica del punto y del área donde se encuentra
				PointInAreaAlert pointInAreaAlert = new PointInAreaAlert();
				pointInAreaAlert.setVesselMmsi(pointRecord.get("mmsi").toString());
				pointInAreaAlert.setVesselName(pointRecord.get("name").toString());
				pointInAreaAlert.setGeometry(pointRecord.get("geometry").toString());
				pointInAreaAlert.setDateTime(
						new DateTime(Long.parseLong(pointRecord.get("dateTime").toString()), DateTimeZone.UTC));
				pointInAreaAlert.setVesselType(Integer.parseInt(pointRecord.get("vesselType").toString()));
				pointInAreaAlert.setAreaId(areaRecord.get("id").toString());
				pointInAreaAlert.setAreaName(areaRecord.get("name").toString());

				result.add(pointInAreaAlert);
			}

		}
		return result;
	}

	public static void main(String[] args) {

		Map<String, Object> env = getEnvVariables(requiredVariables);

		// @formatter:off

    	String areasTopic = (String) env.get(AREAS_TOPIC),
    			pointsTopic = (String) env.get(POINTS_TOPIC),
    			resultTopic = (String) env.get(RESULT_TOPIC),
				appId = (String) env.get(APP_ID),
				bootstrapServers = (String) env.get(BOOTSTRAP_SERVERS),
				schemaRegistryUrl = (String) env.get(SCHEMA_REGISTRY);
    	// @formatter:on

		System.out.format("Load config...%n");
		System.out.format("%s: %s%n", requiredVariables.get(AREAS_TOPIC), areasTopic);
		System.out.format("%s: %s%n", requiredVariables.get(POINTS_TOPIC), pointsTopic);
		System.out.format("%s: %s%n", requiredVariables.get(RESULT_TOPIC), resultTopic);

		VesselRestrictionCheckerApplication app = new VesselRestrictionCheckerApplication(
				new CachedSchemaRegistryClient(schemaRegistryUrl, 100), schemaRegistryUrl);

		System.out.format("Kafka streams starting...%n");
		System.out.format("BootstrapServers: %s, SchemaRegistry: %s, AppId: %s%n", bootstrapServers, schemaRegistryUrl,
				appId);

		Topology topology = app.getTopology(pointsTopic, areasTopic, resultTopic);

		Properties props = app.getKafkaProperties(appId, bootstrapServers);

		app.startStreams(topology, props);
	}
}
