package es.redmic.vesselrestrictionchecker.avro.hashmapserde;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class HashMapSerde<K, V> implements Serde<HashMap<K, V>> {

	private final Serde<HashMap<K, V>> inner;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public HashMapSerde(String schemaRegistryUrl) {

		Properties defaultConfig = new Properties();
		defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		defaultConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

		SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);

		KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistry, new HashMap(defaultConfig));

		KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));

		inner = Serdes.serdeFrom(new HashMapSerializer<>(serializer), new HashMapDeserializer<>(deserializer));
	}

	@Override
	public Serializer<HashMap<K, V>> serializer() {
		return inner.serializer();
	}

	@Override
	public Deserializer<HashMap<K, V>> deserializer() {
		return inner.deserializer();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		inner.serializer().configure(configs, isKey);
		inner.deserializer().configure(configs, isKey);
	}

	@Override
	public void close() {
		inner.serializer().close();
		inner.deserializer().close();
	}
}
