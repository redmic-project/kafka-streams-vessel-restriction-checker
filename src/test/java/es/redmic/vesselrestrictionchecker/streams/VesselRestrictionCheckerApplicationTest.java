package es.redmic.vesselrestrictionchecker.streams;

import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.spatial4j.exception.InvalidShapeException;

import es.redmic.vesselrestrictionchecker.VesselRestrictionCheckerApplication;
import es.redmic.vesselrestrictionchecker.dto.PointInAreaAlert;
import es.redmic.vesselrestrictionchecker.utils.GeoUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class VesselRestrictionCheckerApplicationTest {

	// @formatter:off

	private static final String POINT_TOPIC = "pointsTopic",
			AREAS_TOPIC = "areasTopic",
			RESULT_TOPIC = "resultTopic",
			SCHEMA_REGISTRY_URL = "http://dummy",
			AUTO_OFFSET_RESET = "earliest";

	// @formatter:on

	private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

	private TopologyTestDriver testDriver;

	SpecificAvroSerde<AISTrackingDTO> aisTrackingDTOSerde = new SpecificAvroSerde<AISTrackingDTO>(schemaRegistryClient);

	SpecificAvroSerde<AreaDTO> areaDTOSerde = new SpecificAvroSerde<AreaDTO>(schemaRegistryClient);

	SpecificAvroSerde<PointInAreaAlert> pointInAreaAlertSerde = new SpecificAvroSerde<PointInAreaAlert>(
			schemaRegistryClient);

	private ConsumerRecordFactory<String, AISTrackingDTO> aisTrackingDTORecordFactory = new ConsumerRecordFactory<>(
			new StringSerializer(), aisTrackingDTOSerde.serializer());

	private ConsumerRecordFactory<String, AreaDTO> areaDTORecordFactory = new ConsumerRecordFactory<>(
			new StringSerializer(), areaDTOSerde.serializer());

	private final Deserializer<String> stringDeserializer = new StringDeserializer();

	private final Deserializer<PointInAreaAlert> pointInAreaAlertDeserializer = pointInAreaAlertSerde.deserializer();

	@Before
	public void setUp() throws IOException, RestClientException {

		VesselRestrictionCheckerApplication app = new VesselRestrictionCheckerApplication(schemaRegistryClient,
				SCHEMA_REGISTRY_URL);

		Topology topology = app.getTopology(POINT_TOPIC, AREAS_TOPIC, RESULT_TOPIC);

		Properties props = app.getKafkaProperties("appId", "localhost:9092", AUTO_OFFSET_RESET);

		testDriver = new TopologyTestDriver(topology, props);

		schemaRegistryClient.register(POINT_TOPIC + "-value", AISTrackingDTO.SCHEMA$);

		schemaRegistryClient.register(AREAS_TOPIC + "-value", AreaDTO.SCHEMA$);

		pointInAreaAlertDeserializer.configure(
				Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL),
				false);
	}

	@After
	public void tearDown() {
		testDriver.close();
	}

	@Test
	public void vesselRestrictionChecker_SendPointInAreaAlert_IfAreaContainsPoint()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 28.123415162762214, -16.89305824790779);

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))");

		PointInAreaAlert resultExpected = getPointInAreaAlert(ais, area);

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		OutputVerifier.compareKeyValue(
				testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer),
				resultExpected.getVesselMmsi(), resultExpected);

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	@Test
	public void vesselRestrictionChecker_NotSendAnyAlerts_IfAreaNotContainsPoint()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 45.320172290013765, -16.89305824790779);

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))");

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	@Test
	public void vesselRestrictionChecker_Send2PointInAreaAlert_If2AreasContainsPoint()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 28.123415162762214, -16.89305824790779);

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))");

		AreaDTO area2 = getAreaDTO("23",
				"POLYGON((-17.094988975304886 28.097925030710133,-16.85740962960176 28.216580451923424,"
						+ "-16.781878623742386 28.088233070532734,-17.094988975304886 28.097925030710133))");

		AreaDTO area3 = getAreaDTO("24",
				"POLYGON((-17.015338096398636 27.974287046117606,-16.71733394600801 27.907560606132442,"
						+ "-16.93156734444551 27.86507687937166,-17.015338096398636 27.974287046117606))");

		PointInAreaAlert pointInAreaAlert = getPointInAreaAlert(ais, area);

		PointInAreaAlert pointInAreaAlert2 = getPointInAreaAlert(ais, area2);

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area2.getId(), area2));

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area3.getId(), area3));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		OutputVerifier.compareKeyValue(
				testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer),
				pointInAreaAlert.getVesselMmsi(), pointInAreaAlert);

		OutputVerifier.compareKeyValue(
				testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer),
				pointInAreaAlert2.getVesselMmsi(), pointInAreaAlert2);

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	@Test
	public void vesselRestrictionChecker_SendOnlyOnePointInAreaAlert_IfOverwriteSecondArea()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 28.123415162762214, -16.89305824790779);

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))");

		AreaDTO area2 = getAreaDTO("23",
				"POLYGON((-17.094988975304886 28.097925030710133,-16.85740962960176 28.216580451923424,"
						+ "-16.781878623742386 28.088233070532734,-17.094988975304886 28.097925030710133))");

		AreaDTO area3 = getAreaDTO("23",
				"POLYGON((-17.015338096398636 27.974287046117606,-16.71733394600801 27.907560606132442,"
						+ "-16.93156734444551 27.86507687937166,-17.015338096398636 27.974287046117606))");

		PointInAreaAlert pointInAreaAlert = getPointInAreaAlert(ais, area);

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area2.getId(), area2));

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area3.getId(), area3));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		OutputVerifier.compareKeyValue(
				testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer),
				pointInAreaAlert.getVesselMmsi(), pointInAreaAlert);

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	@Test
	public void vesselRestrictionChecker_SendPointInAreaAlert_IfAreaContainsVesselAndNotFulfillVesselTypeConstraint()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 28.123415162762214, -16.89305824790779);

		List<String> vesselTypesRestricted = new ArrayList<>();
		vesselTypesRestricted.add("2");

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))",
				null, vesselTypesRestricted);

		PointInAreaAlert resultExpected = getPointInAreaAlert(ais, area);

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		OutputVerifier.compareKeyValue(
				testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer),
				resultExpected.getVesselMmsi(), resultExpected);

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	@Test
	public void vesselRestrictionChecker_NoSendAnyAlert_IfAreaContainsVesselAndFulfillVesselTypeConstraint()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 28.123415162762214, -16.89305824790779);

		List<String> vesselTypesRestricted = new ArrayList<>();
		vesselTypesRestricted.add("4");

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))",
				null, vesselTypesRestricted);

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	@Test
	public void vesselRestrictionChecker_SendPointInAreaAlert_IfAreaContainsVesselAndNotFulfillSpeedConstraint()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 28.123415162762214, -16.89305824790779);

		Double maxSpeed = 8.0;

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))",
				maxSpeed, null);

		PointInAreaAlert resultExpected = getPointInAreaAlert(ais, area);

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		OutputVerifier.compareKeyValue(
				testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer),
				resultExpected.getVesselMmsi(), resultExpected);

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	@Test
	public void vesselRestrictionChecker_NoSendAnyAlert_IfAreaContainsVesselAndFulfillSpeedConstraint()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 28.123415162762214, -16.89305824790779);

		Double maxSpeed = 30.0;

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))",
				maxSpeed, null);

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	@Test
	public void vesselRestrictionChecker_SendPointInAreaAlert_IfAreaContainsVesselAndNotFulfillConstraints()
			throws InvalidShapeException, ParseException, InterruptedException {

		AISTrackingDTO ais = getAISTrackingDTO(2, 28.123415162762214, -16.89305824790779);

		Double maxSpeed = 8.0;

		List<String> vesselTypesRestricted = new ArrayList<>();
		vesselTypesRestricted.add("2");

		AreaDTO area = getAreaDTO("22",
				"POLYGON((-17.115923627143275 28.26107051182232,-16.86186478925265 28.268327827045535,"
						+ "-16.7053096134714 27.970373554893733,-17.047259076362025 27.974012125154626,"
						+ "-17.115923627143275 28.26107051182232))",
				maxSpeed, vesselTypesRestricted);

		PointInAreaAlert resultExpected = getPointInAreaAlert(ais, area);

		testDriver.pipeInput(areaDTORecordFactory.create(AREAS_TOPIC, area.getId(), area));

		testDriver.pipeInput(aisTrackingDTORecordFactory.create(POINT_TOPIC, ais.getMmsi().toString(), ais));

		OutputVerifier.compareKeyValue(
				testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer),
				resultExpected.getVesselMmsi(), resultExpected);

		assertNull(testDriver.readOutput(RESULT_TOPIC, stringDeserializer, pointInAreaAlertDeserializer));
	}

	// TODO: Añadir más tests de posibles casos que vayan apareciendo

	private AISTrackingDTO getAISTrackingDTO(int mmsi, double latitude, double longitude) {

		AISTrackingDTO ais = new AISTrackingDTO();
		ais.setLatitude(latitude);
		ais.setLongitude(longitude);
		ais.setMmsi(mmsi);
		ais.setName("Santa María");
		ais.setType(2);
		ais.setTstamp(DateTime.now());
		ais.setSog(15.0);

		return ais;
	}

	private AreaDTO getAreaDTO(String id, String geometry, Double maxSpeed, List<String> vesselTypesRestricted) {

		AreaDTO area = getAreaDTO(id, geometry);
		area.setMaxSpeed(maxSpeed);
		area.setVesselTypesRestricted(vesselTypesRestricted);
		return area;
	}

	private AreaDTO getAreaDTO(String id, String geometry) {
		AreaDTO area = new AreaDTO();
		area.setId(id);
		area.setName("Área protegida");
		area.setGeometry(geometry);
		return area;
	}

	private PointInAreaAlert getPointInAreaAlert(AISTrackingDTO ais, AreaDTO area)
			throws InvalidShapeException, ParseException {

		PointInAreaAlert resultExpected = new PointInAreaAlert();
		resultExpected.setAreaId(area.getId());
		resultExpected.setAreaName(area.getName());
		resultExpected.setVesselMmsi(ais.getMmsi().toString());
		resultExpected.setVesselName(ais.getName());
		resultExpected.setVesselType(ais.getType());
		resultExpected.setGeometry(GeoUtils.getWKTFromLatLon(ais.getLatitude(), ais.getLongitude()));
		resultExpected.setDateTime(new DateTime(ais.getTstamp(), DateTimeZone.UTC));
		resultExpected.setSog(ais.getSog());

		return resultExpected;
	}
}
