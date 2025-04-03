package org.improving.workshop.phase3;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.KeyValue;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.artist.Artist;

import org.improving.workshop.phase3.TopSellingGenreByVenue.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.improving.workshop.utils.DataFaker.ARTISTS;
import static org.improving.workshop.utils.DataFaker.EVENTS;
import static org.improving.workshop.utils.DataFaker.TICKETS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TopSellingGenreByVenueTest {
    private TopologyTestDriver driver;

    private TestInputTopic<String, Ticket> ticketInputTopic;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Artist> artistInputTopic;
    private TestOutputTopic<String, Artist> artistKTable;
    private TestOutputTopic<String, EventArtist> eventArtistKTable;
    private TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic;

    @BeforeEach
    public void setup() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        TopSellingGenreByVenue.configureTopology(streamsBuilder);

        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        artistInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        );

        artistKTable = driver.createOutputTopic(
                TopSellingGenreByVenue.ARTIST_KTABLE,
                Serdes.String().deserializer(),
                Streams.SERDE_ARTIST_JSON.deserializer()
        );

        eventArtistKTable = driver.createOutputTopic(
                TopSellingGenreByVenue.EVENT_ARTIST_KTABLE,
                Serdes.String().deserializer(),
                TopSellingGenreByVenue.EVENT_ARTIST_JSON_SERDE.deserializer()
        );

        outputTopic = driver.createOutputTopic(
                TopSellingGenreByVenue.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                TopSellingGenreByVenue.LINKED_HASH_MAP_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    public void cleanup() { driver.close(); }

    @Test
    @DisplayName("Check Artist KTable Pipeline")
    public void artist_ktable_write_one_then_read_one() {
        // ARRANGE
        String artistId1 = UUID.randomUUID().toString();
        Artist artist1 = ARTISTS.generate(artistId1);
        //log.info("Created Artist with id '{}' and value '{}'", artistId1, artist1);

        // STREAM INPUTS
        artistInputTopic.pipeInput(artistId1, artist1);

        // STREAM OUTPUTS
        KeyValue<String, Artist> result = artistKTable.readKeyValue();
        //log.info("Output result has key '{}' and value '{}'", result.key, result.value);

        // ASSERT
        assertEquals(artistId1, result.key);
        assertEquals(artist1.id(), result.value.id());
        assertEquals(artist1.name(), result.value.name());
        assertEquals(artist1.genre(), result.value.genre());
    }

    @Test
    @DisplayName("Check EventArtist KTable Pipeline")
    public void event_artist_ktable_write_one_then_read_one(){
        // ARRANGE
        String artistId1 = UUID.randomUUID().toString();
        Artist artist1 = ARTISTS.generate(artistId1);
        //log.info("Created Artist with id '{}' and value '{}'", artistId1, artist1);

        String eventId1 = UUID.randomUUID().toString();
        String venueId1 = UUID.randomUUID().toString();
        int venueCap1 = 500;
        Event event1 = EVENTS.generate(eventId1, artistId1, venueId1, venueCap1);
        //log.info("Created Event with id '{}' and value '{}'", eventId1, event1);

        // STREAM INPUTS
        artistInputTopic.pipeInput(artistId1, artist1);
        eventInputTopic.pipeInput(eventId1, event1);

        // STREAM OUTPUTS
        KeyValue<String, EventArtist> result = eventArtistKTable.readKeyValue();
        //log.info("Output result has key '{}' and value '{}'", result.key, result.value);

        // ASSERT
        assertEquals(eventId1, result.key);
        assertEquals(event1.id(), result.value.event.id());
        assertEquals(event1.artistid(), result.value.event.artistid());
        assertEquals(event1.venueid(), result.value.event.venueid());
        assertEquals(event1.capacity(), result.value.event.capacity());
        assertEquals(event1.eventdate(), result.value.event.eventdate());
        assertEquals(artist1.id(), result.value.artist.id());
        assertEquals(artist1.name(), result.value.artist.name());
        assertEquals(artist1.genre(), result.value.artist.genre());
    }

    @Test
    @DisplayName("Check Aggregation 1")
    public void full_aggregate_check_1() {
        // ARRANGE
        String artistId1 = UUID.randomUUID().toString();
        Artist artist1 = ARTISTS.generate(artistId1);
        //log.info("Created Artist with id '{}' and value '{}'", artistId1, artist1);

        String eventId1 = UUID.randomUUID().toString();
        String venueId1 = UUID.randomUUID().toString();
        int venueCap1 = 500;
        Event event1 = EVENTS.generate(eventId1, artistId1, venueId1, venueCap1);
        //log.info("Created Event with id '{}' and value '{}'", eventId1, event1);

        String ticketId1 = UUID.randomUUID().toString();
        String customerId1 = UUID.randomUUID().toString();
        Ticket ticket1 = TICKETS.generate(ticketId1, customerId1, venueId1);
        //log.info("Created Ticket with id '{}' and value '{}'", ticketId1, ticket1);

        // STREAM INPUTS
        artistInputTopic.pipeInput(artistId1, artist1);
        eventInputTopic.pipeInput(eventId1, event1);
        ticketInputTopic.pipeInput(ticketId1, ticket1);

        // STREAM OUTPUTS
        KeyValue<String, LinkedHashMap<String, Long>> result = outputTopic.readKeyValue();
        //log.info("Output result has key '{}' and value '{}'", result.key, result.value);

        // ASSERT
        //assertEquals()
    }
}