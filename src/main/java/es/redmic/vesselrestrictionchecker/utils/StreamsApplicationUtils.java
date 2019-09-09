package es.redmic.vesselrestrictionchecker.utils;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import es.redmic.vesselrestrictionchecker.common.CustomRocksDBConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class StreamsApplicationUtils {

	// @formatter:off

	protected final static String SCHEMA_REGISTRY_URL_PROPERTY = "schema.registry.url",
			SCHEMA_REGISTRY_VALUE_SUBJECT_NAME_STRATEGY = "value.subject.name.strategy",
			AUTO_OFFSET_RESET = "auto.offset.reset";

	// @formatter:on

	public static Properties getStreamConfig(String appId, String bootstrapServers, String schemaRegistry,
			String autoOffsetReset) {

		Properties config = new Properties();

		config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
		config.put(AUTO_OFFSET_RESET, autoOffsetReset);
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1); // commit as fast as possible

		config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
				LogAndContinueExceptionHandler.class);

		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
		config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
		config.put(SCHEMA_REGISTRY_URL_PROPERTY, schemaRegistry);
		config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		config.put(SCHEMA_REGISTRY_VALUE_SUBJECT_NAME_STRATEGY,
				"io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");

		return config;
	}
}
