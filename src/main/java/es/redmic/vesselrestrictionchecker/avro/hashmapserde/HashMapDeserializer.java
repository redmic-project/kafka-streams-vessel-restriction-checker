package es.redmic.vesselrestrictionchecker.avro.hashmapserde;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class HashMapDeserializer<K, V> implements Deserializer<HashMap<K, V>> {

	KafkaAvroDeserializer deserializer;

	// Default constructor needed by Kafka
	public HashMapDeserializer() {
	}

	public HashMapDeserializer(KafkaAvroDeserializer deserializer) {
		this.deserializer = deserializer;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.deserializer.configure(configs, isKey);
	}

	@SuppressWarnings("unchecked")
	@Override
	public HashMap<K, V> deserialize(String topic, byte[] bytes) {

		if (bytes == null || bytes.length == 0) {
			return null;
		}

		final HashMap<K, V> hashMap = new HashMap<K, V>();
		final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

		try {
			final int records = dataInputStream.readInt();
			for (int i = 0; i < records; i++) {

				final byte[] keyBytes = new byte[dataInputStream.readInt()];
				dataInputStream.read(keyBytes);

				final byte[] valueBytes = new byte[dataInputStream.readInt()];
				dataInputStream.read(valueBytes);

				hashMap.put((K) new String(keyBytes, StandardCharsets.UTF_8),
						(V) deserializer.deserialize(topic, valueBytes));
			}
		} catch (IOException e) {
			throw new RuntimeException("Unable to deserialize ArrayList", e);
		}

		return hashMap;
	}

	@Override
	public void close() {
		this.deserializer.close();
	}
}
