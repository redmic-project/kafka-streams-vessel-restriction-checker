package es.redmic.vesselrestrictionchecker;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
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
			RESULT_VESSEL_MMSI_PROPERTY = "vesselMmsi",
			VESSEL_TYPE_PROPERTY = "vesselType",
			MAX_SPEED_PROPERTY = "maxSpeed",
			VESSEL_TYPES_RESTRICTED_PROPERTY = "vesselTypesRestricted",
			VESSEL_SPEED_PROPERTY = "sog";

	private static final Double SPEED_TOLERANCE = 2.0;
	
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

		KStream<String, GenericRecord> areasStream = builder.stream(areasTopic,
				Consumed.with(null, getGenericAvroSerde()));

		KStream<String, GenericRecord> pointsStream = builder.stream(pointsTopic,
				Consumed.with(null, getGenericAvroSerde()));

		KStream<String, GenericRecord> lastAreasKStream = areasStream
				.groupByKey(Serialized.with(null, getGenericAvroSerde()))
				.reduce((aggValue, newValue) -> newValue, Materialized.with(null, getGenericAvroSerde())).toStream();

		KStream<String, GenericRecord> areasKStreamEnriched = lastAreasKStream
				.flatMapValues((value) -> enrichAreaWithGeoHash(value)).filter((k, v) -> (v != null))
				.selectKey((key, value) -> getGeoHashKey(value));

		KTable<String, HashMap<String, GenericRecord>> areasKTableAgg = areasKStreamEnriched
				.groupByKey(Serialized.with(null, getGenericAvroSerde())).aggregate(HashMap<String, GenericRecord>::new,
						// Agrega las diferentes áreas con el mismo geoHashCode
						(k, v, map) -> AvroUtils.aggregateGenericRecordInMap(k, v, map, "id"),
						Materialized.with(null, hashMapSerde));

		KStream<String, GenericRecord> pointsStreamEnriched = pointsStream
				.mapValues(value -> enrichPointWithGeoHash(value)).filter((k, v) -> (v != null))
				.selectKey((k, v) -> getGeoHashKey(v));

		pointsStreamEnriched
				.join(areasKTableAgg, (point, areas) -> getPointInAreaAlert(point, areas),
						Joined.valueSerde(getGenericAvroSerde()))
				.flatMapValues(value -> value).selectKey((k, v) -> v.get(RESULT_VESSEL_MMSI_PROPERTY))
				.to(resultTopic, Produced.with(null, getSpecificAvroSerde()));
		// TODO: Agregar alertas en los últimos x minutos. De esta forma se evitarán
		// falsas alarmas.

		return builder.build();
	}

	private String getGeoHashKey(GenericRecord v) {
		return v.get(GEO_HASH_KEY).toString();
	}

	private GenericRecord enrichPointWithGeoHash(GenericRecord value) {

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
		avroRecord.put("mmsi", value.get("mmsi").toString());

		Object name = value.get("name");
		if (name != null)
			avroRecord.put("name", name.toString());

		avroRecord.put("dateTime", value.get("tstamp"));
		avroRecord.put("vesselType", value.get("type"));
		avroRecord.put("sog", value.get("sog"));

		return avroRecord;
	}

	private List<GenericRecord> enrichAreaWithGeoHash(GenericRecord value) {

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
					avroRecord.put(RESULT_GEOMETRY_PROPERTY, geometry);
					avroRecord.put("id", value.get("id"));
					avroRecord.put("name", value.get("name"));
					avroRecord.put(VESSEL_TYPES_RESTRICTED_PROPERTY, value.get(VESSEL_TYPES_RESTRICTED_PROPERTY));
					avroRecord.put(MAX_SPEED_PROPERTY, value.get(MAX_SPEED_PROPERTY));
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
			if (GeoUtils.shapeContainsGeometry(area, point) && !fulfillNavigationConstraints(pointRecord, areaRecord)) {

				// Se crea una alerta con la info básica del punto y del área donde se encuentra
				PointInAreaAlert pointInAreaAlert = new PointInAreaAlert();
				pointInAreaAlert.setVesselMmsi(pointRecord.get("mmsi").toString());

				Object name = pointRecord.get("name");
				if (name != null)
					pointInAreaAlert.setVesselName(name.toString());
				pointInAreaAlert.setGeometry(pointRecord.get("geometry").toString());
				pointInAreaAlert.setDateTime(
						new DateTime(Long.parseLong(pointRecord.get("dateTime").toString()), DateTimeZone.UTC));

				Object vesselType = pointRecord.get("vesselType");
				if (vesselType != null)
					pointInAreaAlert.setVesselType(Integer.parseInt(vesselType.toString()));

				pointInAreaAlert.setSog((Double) pointRecord.get("sog"));
				pointInAreaAlert.setAreaId(areaRecord.get("id").toString());
				pointInAreaAlert.setAreaName(areaRecord.get("name").toString());

				result.add(pointInAreaAlert);
			}
		}
		return result;
	}

	@SuppressWarnings({ "rawtypes" })
	private boolean fulfillNavigationConstraints(GenericRecord pointRecord, GenericRecord areaRecord) {

		Integer vesselType = (Integer) pointRecord.get(VESSEL_TYPE_PROPERTY);

		Double vesselSpeed = pointRecord.get(VESSEL_SPEED_PROPERTY) != null
				? ((Double) pointRecord.get(VESSEL_SPEED_PROPERTY))
				: null;

		List<?> vesselTypesRestricted = areaRecord.get(VESSEL_TYPES_RESTRICTED_PROPERTY) != null
				? ((List) areaRecord.get(VESSEL_TYPES_RESTRICTED_PROPERTY))
				: null;

		Double maxSpeed = areaRecord.get(MAX_SPEED_PROPERTY) != null ? ((Double) areaRecord.get(MAX_SPEED_PROPERTY))
				: null;

		boolean fulfillVesselTypeConstraintResult = fulfillVesselTypeConstraint(vesselType, vesselTypesRestricted),
				fulfillSpeedConstraintResult = fulfillSpeedConstraint(vesselSpeed, maxSpeed);

		return (fulfillVesselTypeConstraintResult && fulfillSpeedConstraintResult)
				|| ((vesselTypesRestricted == null || vesselTypesRestricted.size() == 0) && maxSpeed != null
						&& fulfillSpeedConstraintResult);
	}

	private boolean fulfillVesselTypeConstraint(Integer vesselType, List<?> vesselTypesRestricted) {

		if (vesselTypesRestricted == null || vesselTypesRestricted.size() == 0)
			return false;

		return (vesselTypesRestricted.stream().filter(item -> item.toString().equals(vesselType.toString()))
				.count() == 0);
	}

	private boolean fulfillSpeedConstraint(Double vesselSpeed, Double maxSpeed) {
		return maxSpeed == null || (vesselSpeed != null && (vesselSpeed + SPEED_TOLERANCE <= maxSpeed));
	}

	public static void main(String[] args) {

		Map<String, Object> env = getEnvVariables(requiredVariables);

		// @formatter:off

    	String areasTopic = (String) env.get(AREAS_TOPIC),
    			pointsTopic = (String) env.get(POINTS_TOPIC),
    			resultTopic = (String) env.get(RESULT_TOPIC),
				appId = (String) env.get(APP_ID),
				bootstrapServers = (String) env.get(BOOTSTRAP_SERVERS),
				schemaRegistryUrl = (String) env.get(SCHEMA_REGISTRY),
				autoOffsetReset = (String) env.get(AUTO_OFFSET_RESET);
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

		Properties props = app.getKafkaProperties(appId, bootstrapServers, autoOffsetReset);

		app.startStreams(topology, props);
	}
}
