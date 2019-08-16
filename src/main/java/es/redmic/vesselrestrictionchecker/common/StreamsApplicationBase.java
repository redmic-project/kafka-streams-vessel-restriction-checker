package es.redmic.vesselrestrictionchecker.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import es.redmic.vesselrestrictionchecker.utils.StreamsApplicationUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public abstract class StreamsApplicationBase {

	protected String schemaRegistryUrl;

	private SchemaRegistryClient schemaRegistryClient;

	public StreamsApplicationBase(String schemaRegistryUrl) {

		this.schemaRegistryUrl = schemaRegistryUrl;

		this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
	}

	// @formatter:off

	protected static final String APP_ID = "APP_ID",
			BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS",
			SCHEMA_REGISTRY = "SCHEMA_REGISTRY";
	// @formatter:on

	@SuppressWarnings("serial")
	protected static HashMap<String, String> requiredVariablesBase = new HashMap<String, String>() {
		{
			put(APP_ID, "Stream application identifier");
			put(BOOTSTRAP_SERVERS, "Kafka servers");
			put(SCHEMA_REGISTRY, "Schema registry server");
		}
	};

	public void startStreams(Topology topology, Properties props) {

		KafkaStreams streams = new KafkaStreams(topology, props);

		streams.setUncaughtExceptionHandler(
				(Thread thread, Throwable throwable) -> uncaughtException(thread, throwable, streams));

		streams.start();

		addShutdownHookAndBlock(streams);
	}

	public Properties getKafkaProperties(String appId, String bootstrapServers) {

		// Sobrescribir método o añadir aquí properties específicas si fuera necesario
		return StreamsApplicationUtils.getStreamConfig(appId, bootstrapServers, schemaRegistryUrl);
	}

	protected void addShutdownHookAndBlock(KafkaStreams streams) {

		Thread.currentThread().setUncaughtExceptionHandler((t, e) -> uncaughtException(t, e, streams));

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("Stopping stream. SIGTERM signal");
				streams.close();
			}
		}));
	}

	protected void uncaughtException(Thread thread, Throwable throwable, KafkaStreams streams) {

		System.err.println("Error. The stream will stop working " + throwable.getLocalizedMessage());
		throwable.printStackTrace();
		streams.close();
	}

	protected static Map<String, Object> getEnvVariables(HashMap<String, String> variablesRequired) {

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

	protected <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerde() {

		final SpecificAvroSerde<T> valueSerde = new SpecificAvroSerde<>(schemaRegistryClient);
		valueSerde.configure(
				Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
				false);
		return valueSerde;
	}
}
