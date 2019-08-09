package es.redmic.vesselrestrictionchecker.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import es.redmic.vesselrestrictionchecker.utils.StreamsApplicationUtils;

public abstract class StreamsApplicationBase {

	// @formatter:off

	protected static final String APP_ID = "APP_ID",
			BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS",
			SCHEMA_REGISTRY = "SCHEMA_REGISTRY";
	// @formatter:on

	@SuppressWarnings("serial")
	protected static HashMap<String, Object> requiredVariablesBase = new HashMap<String, Object>() {
		{
			put(APP_ID, "Stream application identifier");
			put(BOOTSTRAP_SERVERS, "Kafka servers");
			put(SCHEMA_REGISTRY, "Schema registry server");
		}
	};

	protected static StreamsBuilder builder = new StreamsBuilder();

	protected static void startStream(String appId, String bootstrapServers, String schemaRegistry) {

		System.out.format("Kafka streams starting...%n");
		System.out.format("BootstrapServers: %s, SchemaRegistry: %s, AppId: %s%n", bootstrapServers, schemaRegistry,
				appId);

		KafkaStreams streams = new KafkaStreams(builder.build(),
				StreamsApplicationUtils.getStreamConfig(appId, bootstrapServers, schemaRegistry));

		streams.setUncaughtExceptionHandler(
				(Thread thread, Throwable throwable) -> uncaughtException(thread, throwable, streams));

		streams.start();

		addShutdownHookAndBlock(streams);
	}

	protected static void addShutdownHookAndBlock(KafkaStreams streams) {

		Thread.currentThread().setUncaughtExceptionHandler((t, e) -> uncaughtException(t, e, streams));

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("Stopping stream. SIGTERM signal");
				streams.close();
			}
		}));
	}

	protected static void uncaughtException(Thread thread, Throwable throwable, KafkaStreams streams) {

		System.err.println("Error. The stream will stop working " + throwable.getLocalizedMessage());
		throwable.printStackTrace();
		streams.close();
	}

	protected static Map<String, Object> getEnvVariables(HashMap<String, Object> variablesRequired) {

		Map<String, Object> envVariables = new HashMap<>();

		for (String key : variablesRequired.keySet()) {

			String value = System.getenv(key);

			if (value == null) {
				System.err.println("Error=Enviroment variable " + key + " not assigned. Description: "
						+ variablesRequired.get(key));
				System.exit(1);
			}
			envVariables.put(key, value);
		}

		return envVariables;
	}
}
