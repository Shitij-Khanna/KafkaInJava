package com.kafkapoc.streams.favcolor;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class FavouriteColourApp {

	public static void main(String[] args) {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-java");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9093");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//		config.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "127.0.0.1:9092");

		// we disable the cache to demonstrate all the "steps" involved in the
		// transformation - not recommended in prod
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

		KStreamBuilder builder = new KStreamBuilder();
		
		// Step 1: We create the topic of users keys to colours
		KStream<String, String> usersAndColours = builder.stream("favourite-colour-input");
		// 1 - we ensure that a comma is here as we will split on it
		usersAndColours.filter((key, value) -> value.contains(","))
		// 2 - we select a key that will be the user id (lowercase for safety)
		.selectKey((key, value) -> value.split(",")[0].toLowerCase())
		// 3 - we get the colour from the value (lowercase for safety)
		.mapValues(value -> value.split(",")[1].toLowerCase());
		// 4 - we filter undesired colours (could be a data sanitization step
//		.filter((user, color)-> Arrays.asList("green","blue","red").contains(color));
		
		usersAndColours.to("user-keys-and-colours");
		
		// step 2 - we read that topic as a KTable so that updates are read correctly
		KTable<String, String> usersAndColorTable = builder.table("user-keys-and-colours");
		
        // step 3 - we count the occurrences of colours
		KTable<String, Long> favouriteColours = usersAndColorTable.groupBy((user, color)-> new KeyValue<>(color, color)).count();
		
		// 6 - we output the results to a Kafka Topic - don't forget the serializers
		favouriteColours.to(Serdes.String(), Serdes.Long(), "favourite-colour-output");
		
		KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
		
		 // only do this in dev - not in prod
		kafkaStreams.cleanUp();
		kafkaStreams.start();

        // print the topology
        System.out.println(kafkaStreams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}
}
