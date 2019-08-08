package es.redmic.kafkastreams.vesselrestrictionchecker.common;

import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import es.redmic.kafkastreams.vesselrestrictionchecker.utils.StreamsApplicationUtils;

public abstract class StreamsApplicationBase {
	
	@SuppressWarnings("serial")
	protected static ArrayList<Option> commandLineBaseOptions = new ArrayList<Option>() {{
	    add(new Option("appId", true, "stream application identifier"));
	    add(new Option("bootstrapServers", true, "kafka servers"));
	    add(new Option("schemaRegistry", true, "schema registry server"));
	}};
	
	protected static StreamsBuilder builder = new StreamsBuilder();
	
	protected static void startStream(CommandLine cmd) {
		
		KafkaStreams streams = new KafkaStreams(builder.build(),
        		StreamsApplicationUtils.getStreamConfig(cmd));
        
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
				System.out.println("Parando stream por señal SIGTERM");
				streams.close();
			}
		}));
	}

	protected static void uncaughtException(Thread thread, Throwable throwable, KafkaStreams streams) {

		String msg = "Error no conocido en kafka stream. El stream dejará de funcionar "
				+ throwable.getLocalizedMessage();
		System.out.println(msg);
		throwable.printStackTrace();
		streams.close();
	}
}
