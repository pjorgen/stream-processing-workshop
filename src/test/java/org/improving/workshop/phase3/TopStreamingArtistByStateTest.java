package org.improving.workshop.phase3;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.improving.workshop.Streams.SERDE_STREAM_JSON;
import static org.improving.workshop.utils.DataFaker.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

//=====================================================================================

@Slf4j
public class TopStreamingArtistByStateTest {

    private TopologyTestDriver testDriver;
    private StreamsBuilder streamsBuilder;
    private TestInputTopic<String, Stream> streamInputTopic;
    private TestInputTopic<String, Artist> artistInputTopic;
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestOutputTopic<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> enrichedArtistCustomerTopic;
    private TestOutputTopic<String, TopStreamingArtistByState.EnrichedStream> outputTopic;
    private KeyValueStore<String, Stream> streamStore;
    private KeyValueStore<String, Artist> artistStore;
//    private StreamFaker streamFaker;
//    private ArtistFaker artistFaker;

    private TestInputTopic<String, Address> addressInputTopic;
    private TestOutputTopic<String, TopStreamingArtistByState.EnrichedArtistAddressStream> enrichedArtistAddressTopic;
    private TestOutputTopic<String, TopStreamingArtistByState.ArtistStateStreamCount> artistStateStreamCountTopic;

    //=====================================================================================
    // Before Each
    //=====================================================================================
    @BeforeEach
    public void setup() {
        // Create the test topology
        streamsBuilder = new StreamsBuilder();
        TopStreamingArtistByState.configureTopology(streamsBuilder);

        // Configure the test driver
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "test-kafka-streams-" + UUID.randomUUID());
        props.put(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);

        // Create test topics
        streamInputTopic = testDriver.createInputTopic(
                TopStreamingArtistByState.STREAM_INPUT_TOPIC,
                Serdes.String().serializer(),
                SERDE_STREAM_JSON.serializer()
        );

        artistInputTopic = testDriver.createInputTopic(
                TopStreamingArtistByState.ARTIST_INPUT_TOPIC,
                Serdes.String().serializer(),
                TopStreamingArtistByState.ARTISTS_JSON_SERDE.serializer()
        );

        customerInputTopic = testDriver.createInputTopic(
                TopStreamingArtistByState.CUSTOMER_INPUT_TOPIC,
                Serdes.String().serializer(),
                TopStreamingArtistByState.SERDE_CUSTOMER_JSON.serializer()
        );

        enrichedArtistCustomerTopic = testDriver.createOutputTopic(
                TopStreamingArtistByState.ENRICHED_ARTIST_CUSTOMER_STREAM_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.ENRICHED_ARTIST_CUSTOMER_STREAM_JSON_SERDE.deserializer()
        );

        addressInputTopic = testDriver.createInputTopic(
                TopStreamingArtistByState.ADDRESS_INPUT_TOPIC,
                Serdes.String().serializer(),
                TopStreamingArtistByState.SERDE_ADDRESS_JSON.serializer()
        );

        enrichedArtistAddressTopic = testDriver.createOutputTopic(
                TopStreamingArtistByState.ENRICHED_ARTIST_ADDRESS_STREAM_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.ENRICHED_ARTIST_ADDRESS_STREAM_JSON_SERDE.deserializer()
        );

        artistStateStreamCountTopic = testDriver.createOutputTopic(
                TopStreamingArtistByState.ARTIST_STATE_STREAM_COUNT_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.ARTIST_STATE_STREAM_COUNT_JSON_SERDE.deserializer()
        );

        outputTopic = testDriver.createOutputTopic(
                TopStreamingArtistByState.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.ENRICHED_STREAM_JSON_SERDE.deserializer()
        );

        // Get access to the state stores
        streamStore = testDriver.getKeyValueStore(TopStreamingArtistByState.STREAM_KTABLE);
        artistStore = testDriver.getKeyValueStore(TopStreamingArtistByState.ARTISTS_KTABLE);
    }

    //=====================================================================================
    // After Each
    //=====================================================================================

    @AfterEach
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    //=====================================================================================
    // Test 1
    //=====================================================================================

    @Test
    @DisplayName("Stream Table KTable Pipeline")
    public void stream_write_one_then_read_one() {
        // ARRANGE
        String customerId = "customer-1";
        String artistId = "artist-1";


        Stream stream = STREAMS.generate(customerId,artistId);
        log.info("Created stream with id '{}' and value '{}'", stream.id(), stream);

        Artist artist = ARTISTS.generate(artistId);
        log.info("Created artist with id '{}' and value '{}'", artistId, artist);

        // ACT - Input data
        // Note: Order matters! Artist must be processed before stream to ensure the join works
        artistInputTopic.pipeInput(artistId, artist);
        streamInputTopic.pipeInput(stream.id(), stream);

        // ASSERT - Verify stream is stored in KTable
        Stream storedStream = streamStore.get(stream.id());
        log.info("Stream from store has key '{}' and value '{}'", stream.id(), storedStream);
        assertNotNull(storedStream, "Stream should be stored in KTable");
        assertEquals(stream.id(), storedStream.id());

        // Verify artist is stored in KTable
        Artist storedArtist = artistStore.get(artistId);
        log.info("Artist from store has key '{}' and value '{}'", artistId, storedArtist);
        assertNotNull(storedArtist, "Artist should be stored in KTable");
        assertEquals(artistId, storedArtist.id());

        // Verify enriched output
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result = outputTopic.readRecord();
        log.info("Output result has key '{}' and value '{}'", result.key(), result.value());

        // ASSERT - Verify result
        assertEquals(artistId, result.key());
        assertNotNull(result.value(), "Enriched stream should not be null");
        assertEquals(stream.id(), result.value().getStream().id());
        assertEquals(artist.id(), result.value().getArtist().id());
    }

    //=====================================================================================
    // Test 2
    //=====================================================================================

    @Test
    @DisplayName("Multiple Streams with the Same Artist")
    public void multiple_streams_same_artist() {
        // ARRANGE
        String artistId = "artist-2";
        Artist artist = ARTISTS.generate(artistId);

        String streamId1 = "stream-2a";
        String streamId2 = "stream-2b";
        String customerId1 = "customer-2a";
        String customerId2 = "customer-2b";

        Stream stream1 = STREAMS.generate(customerId1,artistId);
        Stream stream2 = STREAMS.generate(customerId2,artistId);

        // ACT - Input data
        artistInputTopic.pipeInput(artistId, artist);
        streamInputTopic.pipeInput(streamId1, stream1);
        streamInputTopic.pipeInput(streamId2, stream2);

        // ASSERT - Verify output records
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result1 = outputTopic.readRecord();
        assertEquals(artistId, result1.key());
        assertEquals(stream1.id(), result1.value().getStream().id());

        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result2 = outputTopic.readRecord();
        assertEquals(artistId, result2.key());
        assertEquals(stream2.id(), result2.value().getStream().id());

        assertEquals(0, outputTopic.getQueueSize());
    }


    //=====================================================================================
    // Test 3
    //=====================================================================================

    @Test
    @DisplayName("Multiple Streams with Multiple Artists")
    public void multiple_streams_multiple_artists() {
        // ARRANGE
        // First artist and associated streams
        String artistId1 = "artist-3a";
        Artist artist1 = ARTISTS.generate(artistId1);

        String streamId1 = "stream-3a";
        String streamId2 = "stream-3b";
        String customerId1 = "customer-3a";
        String customerId2 = "customer-3b";

        Stream stream1 = STREAMS.generate(customerId1, artistId1);
        Stream stream2 = STREAMS.generate(customerId2, artistId1);

        // Second artist and associated streams
        String artistId2 = "artist-3c";
        Artist artist2 = ARTISTS.generate(artistId2);

        String streamId3 = "stream-3c";
        String streamId4 = "stream-3d";
        String customerId3 = "customer-3c";
        String customerId4 = "customer-3d";

        Stream stream3 = STREAMS.generate(customerId3, artistId2);
        Stream stream4 = STREAMS.generate(customerId4, artistId2);

        // ACT - Input data
        // Add artists first
        artistInputTopic.pipeInput(artistId1, artist1);
        artistInputTopic.pipeInput(artistId2, artist2);

        // Add streams
        streamInputTopic.pipeInput(streamId1, stream1);
        streamInputTopic.pipeInput(streamId2, stream2);
        streamInputTopic.pipeInput(streamId3, stream3);
        streamInputTopic.pipeInput(streamId4, stream4);

        // ASSERT - Verify output records
        // Validate the stream1 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result1 = outputTopic.readRecord();
        assertEquals(artistId1, result1.key());
        assertEquals(stream1.id(), result1.value().getStream().id());
        assertEquals(artist1.id(), result1.value().getArtist().id());

        // Validate the stream2 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result2 = outputTopic.readRecord();
        assertEquals(artistId1, result2.key());
        assertEquals(stream2.id(), result2.value().getStream().id());
        assertEquals(artist1.id(), result2.value().getArtist().id());

        // Validate the stream3 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result3 = outputTopic.readRecord();
        assertEquals(artistId2, result3.key());
        assertEquals(stream3.id(), result3.value().getStream().id());
        assertEquals(artist2.id(), result3.value().getArtist().id());

        // Validate the stream4 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result4 = outputTopic.readRecord();
        assertEquals(artistId2, result4.key());
        assertEquals(stream4.id(), result4.value().getStream().id());
        assertEquals(artist2.id(), result4.value().getArtist().id());

        assertEquals(0, outputTopic.getQueueSize());
    }

    //=====================================================================================
    // Test 4
    //=====================================================================================

    @Test
    @DisplayName("Multiple Streams with Multiple Artists and Customers")
    public void multiple_streams_multiple_artists_multiple_customers() {
        // ARRANGE
        // First artist and associated streams
        String artistId1 = "artist-3a";
        Artist artist1 = ARTISTS.generate(artistId1);

        String streamId1 = "stream-3a";
        String streamId2 = "stream-3b";
        String customerId1 = "customer-3a";
        String customerId2 = "customer-3b";

        Stream stream1 = STREAMS.generate(customerId1, artistId1);
        Stream stream2 = STREAMS.generate(customerId2, artistId1);

        // Customers
        Customer customer1 = CUSTOMERS.generate(customerId1);
        Customer customer2 = CUSTOMERS.generate(customerId2);

        // Second artist and associated streams
        String artistId2 = "artist-3c";
        Artist artist2 = ARTISTS.generate(artistId2);

        String streamId3 = "stream-3c";
        String streamId4 = "stream-3d";
        String customerId3 = "customer-3c";
        String customerId4 = "customer-3d";

        Stream stream3 = STREAMS.generate(customerId3, artistId2);
        Stream stream4 = STREAMS.generate(customerId4, artistId2);

        // More customers
        Customer customer3 = CUSTOMERS.generate(customerId3);
        Customer customer4 = CUSTOMERS.generate(customerId4);

        // ACT - Input data
        // Add artists first
        artistInputTopic.pipeInput(artistId1, artist1);
        artistInputTopic.pipeInput(artistId2, artist2);

        // Add customers
        customerInputTopic.pipeInput(customerId1, customer1);
        customerInputTopic.pipeInput(customerId2, customer2);
        customerInputTopic.pipeInput(customerId3, customer3);
        customerInputTopic.pipeInput(customerId4, customer4);

        // Add streams
        streamInputTopic.pipeInput(streamId1, stream1);
        streamInputTopic.pipeInput(streamId2, stream2);
        streamInputTopic.pipeInput(streamId3, stream3);
        streamInputTopic.pipeInput(streamId4, stream4);

        // ASSERT - Verify output records for enriched stream
        // Validate the stream1 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result1 = outputTopic.readRecord();
        assertEquals(artistId1, result1.key());
        assertEquals(stream1.id(), result1.value().getStream().id());
        assertEquals(artist1.id(), result1.value().getArtist().id());

        // Validate the stream2 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result2 = outputTopic.readRecord();
        assertEquals(artistId1, result2.key());
        assertEquals(stream2.id(), result2.value().getStream().id());
        assertEquals(artist1.id(), result2.value().getArtist().id());

        // Validate the stream3 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result3 = outputTopic.readRecord();
        assertEquals(artistId2, result3.key());
        assertEquals(stream3.id(), result3.value().getStream().id());
        assertEquals(artist2.id(), result3.value().getArtist().id());

        // Validate the stream4 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result4 = outputTopic.readRecord();
        assertEquals(artistId2, result4.key());
        assertEquals(stream4.id(), result4.value().getStream().id());
        assertEquals(artist2.id(), result4.value().getArtist().id());

        assertEquals(0, outputTopic.getQueueSize());

        // ASSERT - Verify output records for enriched artist customer stream
        // Validate the stream1 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult1 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId1, customerResult1.key());
        assertEquals(stream1.id(), customerResult1.value().getEnrichedStream().getStream().id());
        assertEquals(artist1.id(), customerResult1.value().getEnrichedStream().getArtist().id());
        assertEquals(customer1.id(), customerResult1.value().getCustomer().id());

        // Validate the stream2 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult2 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId2, customerResult2.key());
        assertEquals(stream2.id(), customerResult2.value().getEnrichedStream().getStream().id());
        assertEquals(artist1.id(), customerResult2.value().getEnrichedStream().getArtist().id());
        assertEquals(customer2.id(), customerResult2.value().getCustomer().id());

        // Validate the stream3 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult3 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId3, customerResult3.key());
        assertEquals(stream3.id(), customerResult3.value().getEnrichedStream().getStream().id());
        assertEquals(artist2.id(), customerResult3.value().getEnrichedStream().getArtist().id());
        assertEquals(customer3.id(), customerResult3.value().getCustomer().id());

        // Validate the stream4 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult4 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId4, customerResult4.key());
        assertEquals(stream4.id(), customerResult4.value().getEnrichedStream().getStream().id());
        assertEquals(artist2.id(), customerResult4.value().getEnrichedStream().getArtist().id());
        assertEquals(customer4.id(), customerResult4.value().getCustomer().id());

        assertEquals(0, enrichedArtistCustomerTopic.getQueueSize());
    }

    //=====================================================================================
    // Test 5
    //=====================================================================================

    @Test
    @DisplayName("Multiple Streams with Multiple Artists, Customers, and Addresses")
    public void multiple_streams_multiple_artists_customers_addresses() {
        // ARRANGE
        // First artist and associated streams
        String artistId1 = "artist-4a";
        Artist artist1 = ARTISTS.generate(artistId1);

        String streamId1 = "stream-4a";
        String streamId2 = "stream-4b";
        String customerId1 = "customer-4a";
        String customerId2 = "customer-4b";

        Stream stream1 = STREAMS.generate(customerId1, artistId1);
        Stream stream2 = STREAMS.generate(customerId2, artistId1);

        // Customers
        Customer customer1 = CUSTOMERS.generate(customerId1);
        Customer customer2 = CUSTOMERS.generate(customerId2);

        // Addresses
        Address address1 = ADDRESSES.generateCustomerAddress(customerId1);
        Address address2 = ADDRESSES.generateCustomerAddress(customerId2);

        // Second artist and associated streams
        String artistId2 = "artist-4c";
        Artist artist2 = ARTISTS.generate(artistId2);

        String streamId3 = "stream-4c";
        String streamId4 = "stream-4d";
        String customerId3 = "customer-4c";
        String customerId4 = "customer-4d";

        Stream stream3 = STREAMS.generate(customerId3, artistId2);
        Stream stream4 = STREAMS.generate(customerId4, artistId2);

        // More customers
        Customer customer3 = CUSTOMERS.generate(customerId3);
        Customer customer4 = CUSTOMERS.generate(customerId4);

        // More addresses
        Address address3 = ADDRESSES.generateCustomerAddress(customerId3);
        Address address4 = ADDRESSES.generateCustomerAddress(customerId4);

        // ACT - Input data
        // Add artists first
        artistInputTopic.pipeInput(artistId1, artist1);
        artistInputTopic.pipeInput(artistId2, artist2);

        // Add customers
        customerInputTopic.pipeInput(customerId1, customer1);
        customerInputTopic.pipeInput(customerId2, customer2);
        customerInputTopic.pipeInput(customerId3, customer3);
        customerInputTopic.pipeInput(customerId4, customer4);

        // Add addresses
        addressInputTopic.pipeInput(customerId1, address1);
        addressInputTopic.pipeInput(customerId2, address2);
        addressInputTopic.pipeInput(customerId3, address3);
        addressInputTopic.pipeInput(customerId4, address4);

        // Add streams
        streamInputTopic.pipeInput(streamId1, stream1);
        streamInputTopic.pipeInput(streamId2, stream2);
        streamInputTopic.pipeInput(streamId3, stream3);
        streamInputTopic.pipeInput(streamId4, stream4);

        // ASSERT - Verify output records for enriched stream
        // Validate the stream1 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result1 = outputTopic.readRecord();
        assertEquals(artistId1, result1.key());
        assertEquals(stream1.id(), result1.value().getStream().id());
        assertEquals(artist1.id(), result1.value().getArtist().id());

        // Validate the stream2 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result2 = outputTopic.readRecord();
        assertEquals(artistId1, result2.key());
        assertEquals(stream2.id(), result2.value().getStream().id());
        assertEquals(artist1.id(), result2.value().getArtist().id());

        // Validate the stream3 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result3 = outputTopic.readRecord();
        assertEquals(artistId2, result3.key());
        assertEquals(stream3.id(), result3.value().getStream().id());
        assertEquals(artist2.id(), result3.value().getArtist().id());

        // Validate the stream4 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result4 = outputTopic.readRecord();
        assertEquals(artistId2, result4.key());
        assertEquals(stream4.id(), result4.value().getStream().id());
        assertEquals(artist2.id(), result4.value().getArtist().id());

        assertEquals(0, outputTopic.getQueueSize());

        // ASSERT - Verify output records for enriched artist customer stream
        // Validate the stream1 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult1 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId1, customerResult1.key());
        assertEquals(stream1.id(), customerResult1.value().getEnrichedStream().getStream().id());
        assertEquals(artist1.id(), customerResult1.value().getEnrichedStream().getArtist().id());
        assertEquals(customer1.id(), customerResult1.value().getCustomer().id());

        // Validate the stream2 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult2 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId2, customerResult2.key());
        assertEquals(stream2.id(), customerResult2.value().getEnrichedStream().getStream().id());
        assertEquals(artist1.id(), customerResult2.value().getEnrichedStream().getArtist().id());
        assertEquals(customer2.id(), customerResult2.value().getCustomer().id());

        // Validate the stream3 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult3 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId3, customerResult3.key());
        assertEquals(stream3.id(), customerResult3.value().getEnrichedStream().getStream().id());
        assertEquals(artist2.id(), customerResult3.value().getEnrichedStream().getArtist().id());
        assertEquals(customer3.id(), customerResult3.value().getCustomer().id());

        // Validate the stream4 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult4 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId4, customerResult4.key());
        assertEquals(stream4.id(), customerResult4.value().getEnrichedStream().getStream().id());
        assertEquals(artist2.id(), customerResult4.value().getEnrichedStream().getArtist().id());
        assertEquals(customer4.id(), customerResult4.value().getCustomer().id());

        assertEquals(0, enrichedArtistCustomerTopic.getQueueSize());

        // ASSERT - Verify output records for enriched artist address stream
        // Validate the stream1 record with address
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistAddressStream> addressResult1 =
                enrichedArtistAddressTopic.readRecord();
        assertEquals(customerId1, addressResult1.key());
        assertEquals(stream1.id(), addressResult1.value().getEnrichedStream().getStream().id());
        assertEquals(artist1.id(), addressResult1.value().getEnrichedStream().getArtist().id());
        assertEquals(address1.id(), addressResult1.value().getAddress().id());

        // Validate the stream2 record with address
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistAddressStream> addressResult2 =
                enrichedArtistAddressTopic.readRecord();
        assertEquals(customerId2, addressResult2.key());
        assertEquals(stream2.id(), addressResult2.value().getEnrichedStream().getStream().id());
        assertEquals(artist1.id(), addressResult2.value().getEnrichedStream().getArtist().id());
        assertEquals(address2.id(), addressResult2.value().getAddress().id());

        // Validate the stream3 record with address
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistAddressStream> addressResult3 =
                enrichedArtistAddressTopic.readRecord();
        assertEquals(customerId3, addressResult3.key());
        assertEquals(stream3.id(), addressResult3.value().getEnrichedStream().getStream().id());
        assertEquals(artist2.id(), addressResult3.value().getEnrichedStream().getArtist().id());
        assertEquals(address3.id(), addressResult3.value().getAddress().id());

        // Validate the stream4 record with address
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistAddressStream> addressResult4 =
                enrichedArtistAddressTopic.readRecord();
        assertEquals(customerId4, addressResult4.key());
        assertEquals(stream4.id(), addressResult4.value().getEnrichedStream().getStream().id());
        assertEquals(artist2.id(), addressResult4.value().getEnrichedStream().getArtist().id());
        assertEquals(address4.id(), addressResult4.value().getAddress().id());

        assertEquals(0, enrichedArtistAddressTopic.getQueueSize());
    }

    //=====================================================================================
    // Test 6
    //=====================================================================================
    @Test
    @DisplayName("Single Stream with Artist and State Count")
    public void single_stream_artist_state_count() {
        // ARRANGE
        String artistId = "artist-5a";
        String artistName = "Test Artist";
        Artist artist = ARTISTS.generate(artistId);
        artistName = artist.name();


        String streamId = "stream-5a";
        String customerId = "customer-5a";

        Stream stream = STREAMS.generate(customerId, artistId);

        // Customer
        Customer customer = CUSTOMERS.generate(customerId);

        // Address with specific state for testing
        String state = "CA";
        Address address = ADDRESSES.generateCustomerAddress(customerId, state);
        state = address.state();


        // ACT - Input data
        // Add artist first
        artistInputTopic.pipeInput(artistId, artist);

        // Add customer
        customerInputTopic.pipeInput(customerId, customer);

        // Add address
        addressInputTopic.pipeInput(customerId, address);

        // Add stream
        streamInputTopic.pipeInput(streamId, stream);

        // Process the original enriched stream outputs (consume them so we can get to the aggregate)
        outputTopic.readRecord();
        enrichedArtistCustomerTopic.readRecord();
        enrichedArtistAddressTopic.readRecord();

        // ASSERT - Verify output record for artist-state-stream count
        TestRecord<String, TopStreamingArtistByState.ArtistStateStreamCount> countResult =
                artistStateStreamCountTopic.readRecord();

        // The key should be a composite of artistId-state
        String expectedKey = artistId + "-" + state;
        assertEquals(expectedKey, countResult.key());

        // Verify the count record fields
        assertEquals(artistId, countResult.value().getArtistId());
        assertEquals(artistName, countResult.value().getArtistName());
        assertEquals(state, countResult.value().getState());
        assertEquals(1L, countResult.value().getCount());

        assertEquals(0, artistStateStreamCountTopic.getQueueSize());
    }

    //=====================================================================================
    // Test 7
    //=====================================================================================

    @Test
    @DisplayName("Multiple Streams with Multiple Artists and States Count")
    public void multiple_streams_artists_states_count() {
        // ARRANGE - Set up test data for multiple artists and states
        // Define test artists
        String[] artistIds = {"artist-1", "artist-2", "artist-3"};
        Artist[] artists = new Artist[artistIds.length];
        String[] artistNames = new String[artistIds.length];

        // Generate artists
        for (int i = 0; i < artistIds.length; i++) {
            artists[i] = ARTISTS.generate(artistIds[i]);
            artistNames[i] = artists[i].name();
        }

        // Define requested states for customers
        String[] requestedStates = {"CA", "CA", "NY", "TX", "FL", "NY"};

        // Define customers for different states
        String[] customerIds = {"customer-ca1", "customer-ca2", "customer-ny1",
                "customer-tx1", "customer-fl1", "customer-ny2"};
        Customer[] customers = new Customer[customerIds.length];
        Address[] addresses = new Address[customerIds.length];
        String[] actualStates = new String[customerIds.length];

        // Generate customers and their addresses - get actual states from address objects
        for (int i = 0; i < customerIds.length; i++) {
            customers[i] = CUSTOMERS.generate(customerIds[i]);
            // Generate address with requested state
            addresses[i] = ADDRESSES.generateCustomerAddress(customerIds[i], requestedStates[i]);
            // Get the actual state from the address
            actualStates[i] = addresses[i].state();
        }

        // Create multiple streams with different combinations
        // Format: streamId, customerId, artistId
        String[][] streamData = {
                {"stream-1", customerIds[0], artistIds[0]}, // customer-ca1, artist-1
                {"stream-2", customerIds[1], artistIds[0]}, // customer-ca2, artist-1
                {"stream-3", customerIds[2], artistIds[1]}, // customer-ny1, artist-2
                {"stream-4", customerIds[3], artistIds[1]}, // customer-tx1, artist-2
                {"stream-5", customerIds[4], artistIds[2]}, // customer-fl1, artist-3
                {"stream-6", customerIds[5], artistIds[0]}, // customer-ny2, artist-1
                {"stream-7", customerIds[0], artistIds[2]}, // customer-ca1, artist-3
                {"stream-8", customerIds[2], artistIds[2]}  // customer-ny1, artist-3
        };

        Stream[] streams = new Stream[streamData.length];
        for (int i = 0; i < streamData.length; i++) {
            streams[i] = STREAMS.generate(streamData[i][1], streamData[i][2]);
        }

        // ACT - Add all test data to topics

        // Add all artists first
        for (int i = 0; i < artists.length; i++) {
            artistInputTopic.pipeInput(artistIds[i], artists[i]);
        }

        // Add all customers
        for (int i = 0; i < customers.length; i++) {
            customerInputTopic.pipeInput(customerIds[i], customers[i]);
        }

        // Add all addresses
        for (int i = 0; i < addresses.length; i++) {
            addressInputTopic.pipeInput(customerIds[i], addresses[i]);
        }

        // Add all streams
        for (int i = 0; i < streams.length; i++) {
            streamInputTopic.pipeInput(streamData[i][0], streams[i]);

            // Process the enriched stream outputs for each stream
            outputTopic.readRecord();
            enrichedArtistCustomerTopic.readRecord();
            enrichedArtistAddressTopic.readRecord();
        }

        // Build expected counts based on actual states returned by addresses
        Map<String, Map<String, Long>> artistStateCounts = new HashMap<>();

        // Initialize the artist-state count map
        for (String artistId : artistIds) {
            artistStateCounts.put(artistId, new HashMap<>());
        }

        // Count streams by artist and state
        for (String[] stream : streamData) {
            String customerId = stream[1];
            String artistId = stream[2];

            // Find the customer index to get the actual state
            int customerIndex = -1;
            for (int i = 0; i < customerIds.length; i++) {
                if (customerIds[i].equals(customerId)) {
                    customerIndex = i;
                    break;
                }
            }

            if (customerIndex >= 0) {
                String state = actualStates[customerIndex];
                Map<String, Long> stateCounts = artistStateCounts.get(artistId);
                stateCounts.put(state, stateCounts.getOrDefault(state, 0L) + 1L);
            }
        }

        // Create a map to store expected results: key=artistId-state, value=[artistName, count]
        Map<String, Object[]> expectedResults = new HashMap<>();

        // Build expected results from the counts
        for (String artistId : artistIds) {
            int artistIndex = -1;
            for (int i = 0; i < artistIds.length; i++) {
                if (artistIds[i].equals(artistId)) {
                    artistIndex = i;
                    break;
                }
            }

            Map<String, Long> stateCounts = artistStateCounts.get(artistId);
            for (Map.Entry<String, Long> entry : stateCounts.entrySet()) {
                String state = entry.getKey();
                Long count = entry.getValue();
                expectedResults.put(artistId + "-" + state,
                        new Object[]{artistNames[artistIndex], count});
            }
        }

        // Read all the results from the topic
        int expectedRecordCount = expectedResults.size();
        for (int i = 0; i < expectedRecordCount; i++) {
            TestRecord<String, TopStreamingArtistByState.ArtistStateStreamCount> countResult =
                    artistStateStreamCountTopic.readRecord();

            String key = countResult.key();
            TopStreamingArtistByState.ArtistStateStreamCount value = countResult.value();

            // Verify this record matches our expectations
            assertEquals(true, expectedResults.containsKey(key),
                    "Unexpected key found: " + key);

            Object[] expected = expectedResults.get(key);
            assertEquals(expected[0], value.getArtistName(),
                    "Artist name mismatch for key: " + key);
            assertEquals(expected[1], value.getCount(),
                    "Count mismatch for key: " + key);

            // Extract artistId and state from composite key
            // The key format is: "artistId-state" but artistId itself contains a hyphen
            // Find the last hyphen in the string to separate artistId and state
            int lastHyphenIndex = key.lastIndexOf("-");
            String artistId = key.substring(0, lastHyphenIndex);
            String state = key.substring(lastHyphenIndex + 1);

            // Verify the individual components
            assertEquals(artistId, value.getArtistId());
            assertEquals(state, value.getState());

            // Remove the verified entry to ensure we don't count it twice
            expectedResults.remove(key);
        }

        // Verify we've processed all expected records
        assertEquals(true, expectedResults.isEmpty(),
                "Not all expected combinations were found: " + expectedResults.keySet());

        // Verify there are no more records in the topic
        assertEquals(0, artistStateStreamCountTopic.getQueueSize());
    }

}