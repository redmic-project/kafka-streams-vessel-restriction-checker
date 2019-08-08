package es.redmic.vesselrestrictionchecker;

import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.kafka.streams.kstream.KStream;

import es.redmic.vesselrestrictionchecker.common.StreamsApplicationBase;
import es.redmic.vesselrestrictionchecker.utils.StreamsApplicationUtils;


public class VesselRestrictionCheckerApplication extends StreamsApplicationBase {
	
	
	private static final String AREAS_TOPIC = "areasTopic",
			POINTS_TOPIC = "pointsTopic",
			RESULT_TOPIC = "resultTopic";
	
	@SuppressWarnings("serial")
	private static ArrayList<Option> commandLineOptions = new ArrayList<Option>() {{
		addAll(commandLineBaseOptions);
	    add(new Option(AREAS_TOPIC, true, "geofencing topic"));
	    add(new Option(POINTS_TOPIC, true, "points to check topic"));
	    add(new Option(RESULT_TOPIC, true, "result topic"));
	}};
	
	/**
	 *
	 * @param args
	 * 
	 * 
	 */
		
    public static void main(String[] args) {
    	
    	CommandLine commandLineArgs = StreamsApplicationUtils.getCommandLineArgs(args, commandLineOptions);
    	
    	// @formatter:off

    	String areasTopic = commandLineArgs.getOptionValue(AREAS_TOPIC),
    			pointsTopic = commandLineArgs.getOptionValue(POINTS_TOPIC),
    			resultTopic = commandLineArgs.getOptionValue(RESULT_TOPIC);
    	// @formatter:on
    	
        KStream<String, String> areasStream = builder.stream(areasTopic),
        		pointsStream = builder.stream(pointsTopic);
        
        // TODO: crear stream

        startStream(commandLineArgs);
    }
}
