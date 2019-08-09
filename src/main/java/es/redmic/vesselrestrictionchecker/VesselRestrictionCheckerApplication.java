package es.redmic.vesselrestrictionchecker;

import java.util.HashMap;
import java.util.Map;

import es.redmic.vesselrestrictionchecker.common.StreamsApplicationBase;

public class VesselRestrictionCheckerApplication extends StreamsApplicationBase {

	// @formatter:off
	
	private static final String AREAS_TOPIC = "AREAS_TOPIC",
			POINTS_TOPIC = "POINTS_TOPIC",
			RESULT_TOPIC = "RESULT_TOPIC";
	
	// @formatter:on

	@SuppressWarnings("serial")
	private static HashMap<String, Object> requiredVariables = new HashMap<String, Object>() {
		{
			putAll(requiredVariablesBase);
			put(AREAS_TOPIC, "Kafka topic for receiving geofencing areas");
			put(POINTS_TOPIC, "Kafka topic for receiving points to check");
			put(RESULT_TOPIC, "Kafka topic for sending checking result");
		}
	};

	public static void main(String[] args) {

		Map<String, Object> env = getEnvVariables(requiredVariables);

		// @formatter:off

    	String areasTopic = (String) env.get(AREAS_TOPIC),
    			pointsTopic = (String) env.get(POINTS_TOPIC),
    			resultTopic = (String) env.get(RESULT_TOPIC),
				appId = (String) env.get(APP_ID),
				bootstrapServers = (String) env.get(BOOTSTRAP_SERVERS),
				schemaRegistry = (String) env.get(SCHEMA_REGISTRY);
    	// @formatter:on

		System.out.format("Load config...%n");
		System.out.format("%s: %s%n", requiredVariables.get(AREAS_TOPIC), areasTopic);
		System.out.format("%s: %s%n", requiredVariables.get(POINTS_TOPIC), pointsTopic);
		System.out.format("%s: %s%n", requiredVariables.get(RESULT_TOPIC), resultTopic);

		/*-KStream<String, String> areasStream = builder.stream(areasTopic),
				pointsStream = builder.stream(pointsTopic);-*/

		// TODO: crear stream

		startStream(appId, bootstrapServers, schemaRegistry);
	}
}
