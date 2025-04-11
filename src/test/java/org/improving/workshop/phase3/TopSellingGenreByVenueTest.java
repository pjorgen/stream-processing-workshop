package org.improving.workshop.phase3;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import  org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.streams.KeyValue;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.artist.Artist;

import org.improving.workshop.phase3.TopSellingGenreByVenue.*;

import java.util.*;

import static org.improving.workshop.utils.DataFaker.ARTISTS;
import static org.improving.workshop.utils.DataFaker.EVENTS;
import static org.improving.workshop.utils.DataFaker.TICKETS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
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
        String artistId1 = "artist-1";
        Artist artist1 = ARTISTS.generate(artistId1);
        log.info("Created Artist with id '{}' and value '{}'", artistId1, artist1);

        // STREAM INPUTS
        artistInputTopic.pipeInput(artistId1, artist1);

        // STREAM OUTPUTS
        KeyValue<String, Artist> result = artistKTable.readKeyValue();
        log.info("Output result has key '{}' and value '{}'", result.key, result.value);

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
        String artistId1 = "artist-1";
        Artist artist1 = ARTISTS.generate(artistId1);
        log.info("Created Artist with id '{}' and value '{}'", artistId1, artist1);

        String eventId1 = "event-1";
        String venueId1 = "venue-1";
        int venueCap1 = 500;
        Event event1 = EVENTS.generate(eventId1, artistId1, venueId1, venueCap1);
        log.info("Created Event with id '{}' and value '{}'", eventId1, event1);

        // STREAM INPUTS
        artistInputTopic.pipeInput(artistId1, artist1);
        eventInputTopic.pipeInput(eventId1, event1);

        // STREAM OUTPUTS
        KeyValue<String, EventArtist> result = eventArtistKTable.readKeyValue();
        log.info("Output result has key '{}' and value '{}'", result.key, result.value);

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
    @DisplayName("Check Aggregation - Single Ticket - One Venue")
    public void aggregate_check_single_ticket() {
        // ARRANGE
        String artistId1 = "artist-1";
        Artist artist1 = ARTISTS.generate(artistId1);
        log.info("Created Artist with id '{}' and value '{}'", artistId1, artist1);

        String eventId1 = "event-1";
        String venueId1 = "venue-1";
        int venueCap1 = 500;
        Event event1 = EVENTS.generate(eventId1, artistId1, venueId1, venueCap1);
        log.info("Created Event with id '{}' and value '{}'", eventId1, event1);

        String ticketId1 = "ticket-1";
        String customerId1 = "customer-1";
        Ticket ticket1 = TICKETS.generate(ticketId1, customerId1, eventId1);
        log.info("Created Ticket with id '{}' and value '{}'", ticketId1, ticket1);

        // STREAM INPUTS
        artistInputTopic.pipeInput(artistId1, artist1);
        eventInputTopic.pipeInput(eventId1, event1);
        ticketInputTopic.pipeInput(ticketId1, ticket1);

        // STREAM OUTPUTS
        KeyValue<String, LinkedHashMap<String, Long>> result = outputTopic.readKeyValue();
        log.info("Output result has key '{}' and value '{}'", result.key, result.value);

        // ASSERT
        assertEquals(venueId1, result.key);
        assertEquals(artist1.genre(), result.value.entrySet().iterator().next().getKey());
        assertEquals(1, result.value.entrySet().iterator().next().getValue());
    }

    @Test
    @DisplayName("Check Aggregation - Six Tickets - One Venue")
    public void aggregate_check_six_tickets_one_venue() {
        //One venue
        String venueId = "venue-1";

        //Three artists (and three genres)
        String artistId1 = "artist-1";
        String artistId2 = "artist-2";
        String artistId3 = "artist-3";

        // Since genres are chosen randomly by ARTISTS.generate(), manually create the artists
        Artist artist1 = new Artist(artistId1, "Taylor Swift", "Folk");
        Artist artist2 = new Artist(artistId2, "Snow Patrol", "Pop");
        Artist artist3 = new Artist(artistId3, "Glass Animals", "Electronic");

        artistInputTopic.pipeInput(artistId1, artist1);
        artistInputTopic.pipeInput(artistId2, artist2);
        artistInputTopic.pipeInput(artistId3, artist3);

        //Three events, one per artist
        String eventId1 = "event-1";
        String eventId2 = "event-2";
        String eventId3 = "event-3";

        eventInputTopic.pipeInput(eventId1, EVENTS.generate(eventId1, artistId1, venueId, 50));
        eventInputTopic.pipeInput(eventId2, EVENTS.generate(eventId2, artistId2, venueId, 60));
        eventInputTopic.pipeInput(eventId3, EVENTS.generate(eventId3, artistId3, venueId, 70));

        // Six purchased tickets
        ticketInputTopic.pipeInput("ticket-1", TICKETS.generate("customer-1", eventId1));

        // Read output records
        TestRecord<String, LinkedHashMap<String, Long>> outputRecord = outputTopic.readRecord();
        log.info("Output result: '{}'", outputRecord);

        // Check for the expected number of records (1)
        assertEquals(venueId, outputRecord.key());
        assertEquals(artist1.genre(), outputRecord.value().entrySet().iterator().next().getKey());
        assertEquals(1, outputRecord.value().entrySet().iterator().next().getValue());

        // Five more purchased tickets
        ticketInputTopic.pipeInput("ticket-2", TICKETS.generate("customer-2", eventId1));
        ticketInputTopic.pipeInput("ticket-3", TICKETS.generate("customer-3", eventId1));
        ticketInputTopic.pipeInput("ticket-4", TICKETS.generate("customer-1", eventId2));
        ticketInputTopic.pipeInput("ticket-5", TICKETS.generate("customer-2", eventId2));
        ticketInputTopic.pipeInput("ticket-6", TICKETS.generate("customer-3", eventId3));

        // Read output records
        var outputRecords = outputTopic.readRecordsToList();
        log.info("Output result: '{}'", outputRecords);
        log.info("Total number of events: '{}'", outputRecords.size());

        // Check for the expected number of records (5)
        assertEquals(5, outputRecords.size());

        // Create an iterator for the map of the last entry
        Iterator<Map.Entry<String, Long>> iterator = outputRecords.getLast().getValue().entrySet().iterator();
        var entry = iterator.next();

        // Looking just at the last event, check the values against expectations
        assertEquals(venueId, outputRecords.getLast().getKey());
        assertEquals(artist1.genre(), entry.getKey());
        assertEquals(3, entry.getValue());
        entry = iterator.next();
        assertEquals(artist2.genre(), entry.getKey());
        assertEquals(2, entry.getValue());
        entry = iterator.next();
        assertEquals(artist3.genre(), entry.getKey());
        assertEquals(1, entry.getValue());

    }

    @Test
    @DisplayName("Check Aggregation - Three Tickets - Three Venues")
    public void aggregate_check_three_tickets_three_venues() {
        //One venue
        String venueId1 = "venue-1";
        String venueId2 = "venue-2";
        String venueId3 = "venue-3";

        //Three artists (and three genres)
        String artistId1 = "artist-1";
        String artistId2 = "artist-2";
        String artistId3 = "artist-3";

        Artist artist1 = ARTISTS.generate(artistId1);
        Artist artist2 = ARTISTS.generate(artistId2);
        Artist artist3 = ARTISTS.generate(artistId3);

        artistInputTopic.pipeInput(artistId1, artist1);
        artistInputTopic.pipeInput(artistId2, artist2);
        artistInputTopic.pipeInput(artistId3, artist3);

        //Three events, one per artist
        String eventId1 = "event-1";
        String eventId2 = "event-2";
        String eventId3 = "event-3";

        eventInputTopic.pipeInput(eventId1, EVENTS.generate(eventId1, artistId1, venueId1, 50));
        eventInputTopic.pipeInput(eventId2, EVENTS.generate(eventId2, artistId2, venueId2, 60));
        eventInputTopic.pipeInput(eventId3, EVENTS.generate(eventId3, artistId3, venueId3, 70));

        // Three purchased tickets
        ticketInputTopic.pipeInput("ticket-1", TICKETS.generate("customer-1", eventId1));
        ticketInputTopic.pipeInput("ticket-2", TICKETS.generate("customer-2", eventId2));
        ticketInputTopic.pipeInput("ticket-3", TICKETS.generate("customer-3", eventId3));

        // Read output records
        var outputRecords = outputTopic.readRecordsToList();
        log.info("Output result: '{}'", outputRecords);
        log.info("Total number of events: '{}'", outputRecords.size());

        // Check for the expected number of records (3)
        assertEquals(3, outputRecords.size());

        // Check for the expected values
        assertEquals(venueId1, outputRecords.getFirst().getKey());
        assertEquals(artist1.genre(), outputRecords.getFirst().getValue().entrySet().iterator().next().getKey());
        assertEquals(1, outputRecords.getFirst().getValue().entrySet().iterator().next().getValue());
        assertEquals(venueId2, outputRecords.get(1).getKey());
        assertEquals(artist2.genre(), outputRecords.get(1).getValue().entrySet().iterator().next().getKey());
        assertEquals(1, outputRecords.get(1).getValue().entrySet().iterator().next().getValue());
        assertEquals(venueId3, outputRecords.getLast().getKey());
        assertEquals(artist3.genre(), outputRecords.getLast().getValue().entrySet().iterator().next().getKey());
        assertEquals(1, outputRecords.getLast().getValue().entrySet().iterator().next().getValue());

    }

    @Test
    @DisplayName("Check Aggregation - 18 Tickets - Three Venues")
    public void aggregate_check_eighteen_tickets_three_venues() {
        //Three venues
        List<String> venueIds = List.of("venue-1", "venue-2", "venue-3");

        //Three artists (and three genres)
        List<String> artistIds = List.of("artist-1", "artist-2", "artist-3");
        List<Artist> artists = List.of(
                ARTISTS.generate(artistIds.get(0)),
                ARTISTS.generate(artistIds.get(1)),
                ARTISTS.generate(artistIds.get(2))
        );
        for (int i = 0; i <= 2; i++) {
            artistInputTopic.pipeInput(artistIds.get(i), artists.get(i));
        }

        //Nine events, one per artist per venue
        List<String> eventIds = List.of("event-1", "event-2", "event-3", "event-4", "event-5", "event-6", "event-7", "event-8", "event-9");
        for (int i = 0; i < 9; i++) {
            eventInputTopic.pipeInput(eventIds.get(i),
                    EVENTS.generate(eventIds.get(i), artistIds.get(i%3), venueIds.get((i / 3)), 50));
        }


        // Eighteen purchased tickets
        List<String> ticketEvents = List.of(
                "event-1", "event-1", "event-1", "event-2", "event-2", "event-3",
                "event-4", "event-4", "event-4", "event-5", "event-5", "event-6",
                "event-7", "event-7", "event-7", "event-8", "event-8", "event-9");
        for (int i = 1; i <= 18; i++) {
            ticketInputTopic.pipeInput("ticket-" + i,
                    TICKETS.generate("customer-" + i, ticketEvents.get(i-1)));
        }

        // Read output records
        var outputRecords = outputTopic.readRecordsToList();

        // Check for the expected number of records (18)
        assertEquals(18, outputRecords.size());

        // Create an iterator for the map of the last entry
        Iterator<Map.Entry<String, Long>> iterator ;
        Map.Entry<String, Long> entry ;

        for (int i = 5; i < 18; i+=6) {
            // Skip to the last 3 elements of the outputRecords
            // if we did it right the last three cover all three venues
            iterator = outputRecords.get(i).getValue().entrySet().iterator();
            log.info("Expected Venue: '{}'", venueIds.get(i/6));
            log.info("Actual Venue: '{}'", outputRecords.get(i).getKey());
            assertEquals(venueIds.get(i/6), outputRecords.get(i).getKey());
            for(int j = 0, k = 3; j < 3; j++, k--) {
                entry = iterator.next();
                log.info("For j = '{}' and k = '{}'", j, k);
                log.info("Expected genre is: '{}'", artists.get(j).genre());
                log.info("Actual genre: '{}'", entry.getKey());
                assertEquals(artists.get(j).genre(), entry.getKey());
                log.info("Expected ticket count: '{}'", k);
                log.info("Actual ticket count: '{}'", entry.getValue());
                assertEquals(k, entry.getValue());
            }
        }
    }
}