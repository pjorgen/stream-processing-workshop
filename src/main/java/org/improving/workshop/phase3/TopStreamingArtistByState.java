package org.improving.workshop.phase3;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

//===============================================================================================================
// Topics Ktables and Serdes -
//===============================================================================================================

@Slf4j
public class TopStreamingArtistByState {
    // Reference TOPIC_DATA_DEMO_* properties in Streams
    public static final String ADDRESS_INPUT_TOPIC = TOPIC_DATA_DEMO_ADDRESSES;
    public static final String CUSTOMER_INPUT_TOPIC = TOPIC_DATA_DEMO_CUSTOMERS;
    public static final String STREAM_INPUT_TOPIC = TOPIC_DATA_DEMO_STREAMS;
    public static final String ARTIST_INPUT_TOPIC = TOPIC_DATA_DEMO_ARTISTS;

    // KTABLE DEFINITIONS MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String ADDRESS_KTABLE = "kafka-workshop-address-ktable";
    public static final String STREAM_KTABLE = "kafka-workshop-streams-ktable";
    public static final String ARTISTS_KTABLE = "kafka-workshop-artists-ktable";
    public static final String CUSTOMER_ADDRESS_KTABLE = "kafka-workshop-customer-address-ktable";
    public static final String CUSTOMER_ADDRESS_STREAM_KTABLE = "kafka-workshop-customer-address-stream-ktable";
    public static final String OUTPUT_TOPIC = "kafka-workshop-top-streaming-artist-by-state";

    // Serdes
    public static final JsonSerde<Artist> ARTISTS_JSON_SERDE = new JsonSerde<>(Artist.class);
    public static final JsonSerde<CustomerAddress> CUSTOMER_ADDRESS_JSON_SERDE = new JsonSerde<>(CustomerAddress.class);
    public static final JsonSerde<CustomerAddressStream> CUSTOMER_ADDRESS_STREAM_JSON_SERDE = new JsonSerde<>(CustomerAddressStream.class);
    public static final JsonSerde<EnrichedStream> ENRICHED_STREAM_JSON_SERDE = new JsonSerde<>(EnrichedStream.class);
    public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);

    public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE
            = new JsonSerde<>(
            new TypeReference<LinkedHashMap<String, Long>>() {
            },
            new ObjectMapper()
                    .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
    );

    // ===============================================================================================================
    // Main
    // ===============================================================================================================

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    //===============================================================================================================
    // Topology
    //===============================================================================================================

    static void configureTopology(final StreamsBuilder builder) {
        // KTables
        //
        // Streams
        //
        KTable<String, Stream> streamTable = builder
                .table(
                        STREAM_INPUT_TOPIC,
                        Materialized
                                .<String, Stream>as(persistentKeyValueStore(STREAM_KTABLE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_STREAM_JSON)
                );
        streamTable
                .toStream()
                .peek((key, stream) -> log.info("Stream '{}' registered with value '{}'", key, stream))
                .to(STREAM_KTABLE, Produced.with(Serdes.String(), SERDE_STREAM_JSON));

        // Create a KTable from the Artists topic
        KTable<String, Artist> artistTable = builder
                .table(
                        ARTIST_INPUT_TOPIC,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore(ARTISTS_KTABLE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ARTISTS_JSON_SERDE)
                );

        // Log artists as they are loaded
        artistTable
                .toStream()
                .peek((key, artist) -> log.info("Artist '{}' registered with value '{}'", key, artist))
                .to(ARTISTS_KTABLE, Produced.with(Serdes.String(), ARTISTS_JSON_SERDE));

        // Stream table needs to be keyed by artistid to join with artist table
        KStream<String, Stream> streamByArtistId = streamTable
                .toStream()
                .selectKey((streamId, stream) -> stream.artistid());

        // Join the stream with artist table
        KStream<String, EnrichedStream> enrichedStream = streamByArtistId
                .join(
                        artistTable,
                        (stream, artist) -> new EnrichedStream(stream, artist),
                        Joined.with(Serdes.String(), SERDE_STREAM_JSON, ARTISTS_JSON_SERDE)
                );

        // Output to destination topic
        enrichedStream
                .peek((key, value) -> log.info("Enriched Stream: {}", value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), ENRICHED_STREAM_JSON_SERDE));
    }

    //===============================================================================================================
    // Data
    //===============================================================================================================

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EnrichedStream {
        public Stream stream;
        public Artist artist;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerAddress {
        private Customer customer;
        private Address address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerAddressStream {
        private CustomerAddress customerAddress;
        private Stream stream;
    }

    //===============================================================================================================
    // Not sure if this will still be needed - but keep for now
    //===============================================================================================================

    @Data
    @AllArgsConstructor
    public static class SortedCounterMap {
        private int maxSize;
        private LinkedHashMap<String, Long> map;

        public SortedCounterMap() {
            this(1000);
        }

        public SortedCounterMap(int maxSize) {
            this.maxSize = maxSize;
            this.map = new LinkedHashMap<>();
        }

        public void incrementCount(String id) {
            map.compute(id, (k, v) -> v == null ? 1 : v + 1);

            // replace with sorted map
            this.map = map.entrySet().stream()
                    .sorted(reverseOrder(Map.Entry.comparingByValue()))
                    // keep a limit on the map size
                    .limit(maxSize)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
        public LinkedHashMap<String, Long> top(int limit) {
            return map.entrySet().stream()
                    .limit(limit)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }
}