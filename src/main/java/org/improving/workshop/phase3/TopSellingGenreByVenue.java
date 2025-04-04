package org.improving.workshop.phase3;

import com.fasterxml.jackson.core.JsonParser;
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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

import org.improving.workshop.samples.TopCustomerArtists.SortedCounterMap;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.artist.Artist;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class TopSellingGenreByVenue {
    // Reference TOPIC_DATA_DEMO_* properties in Streams
    public static final String ARTIST_INPUT_TOPIC = TOPIC_DATA_DEMO_ARTISTS;
    public static final String EVENT_INPUT_TOPIC = TOPIC_DATA_DEMO_EVENTS;
    public static final String TICKET_INPUT_TOPIC = TOPIC_DATA_DEMO_TICKETS;

    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String ARTIST_KTABLE = "kafka-workshop-artist-ktable";
    public static final String EVENT_ARTIST_KTABLE = "kafka-workshop-event-artist-ktable";
    public static final String OUTPUT_TOPIC = "kafka-workshop-top-selling-genre-by-venue";

    // Set up SERDES
    public static final JsonSerde<EventArtist> EVENT_ARTIST_JSON_SERDE = new JsonSerde<>(EventArtist.class);
    public static final JsonSerde<EnrichedTicket> ENRICHED_TICKET_JSON_SERDE = new JsonSerde<>(EnrichedTicket.class);
    public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);

    // Jackson is converting Value into Integer Not Long due to erasure,
    //public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE = new JsonSerde<>(LinkedHashMap.class);
    public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE
        = new JsonSerde<>(
        new TypeReference<LinkedHashMap<String, Long>>() {
        },
        new ObjectMapper()
            .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
    );

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // Store artists in a table so we can join then to events
        KTable<String, Artist> artistsTable = builder
            .table(
                ARTIST_INPUT_TOPIC,
                Materialized
                    .<String, Artist>as(persistentKeyValueStore(ARTIST_KTABLE))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_ARTIST_JSON)
            );

        // Capture the backside of the table to log a confirmation that the Artist was received
        // Publish it to a stream for testing as well
        artistsTable
            .toStream()
            .peek((key, artist) -> log.info("Artist '{}' registered with value '{}'", key, artist))
            .to(ARTIST_KTABLE, Produced.with(Serdes.String(), SERDE_ARTIST_JSON));

        // Get the event stream, rekey by artistid, and join to the artistsTable
        KTable<String, EventArtist> eventArtistKTable = builder
            .stream(EVENT_INPUT_TOPIC, Consumed.with(Serdes.String(), SERDE_EVENT_JSON))
            .peek((eventId, event)
                -> log.info("Event '{}' registered with value '{}'", eventId, event))

            // Rekey by artistid and mark for repartition
            .selectKey((eventId, event) -> event.artistid(), Named.as("rekey-event-by-artistid"))

            // Join to the artist KTable. Causes a repartition
            .join(
                artistsTable, // Join to table
                (event, artist) // Left value, right value
                    -> new EventArtist(event, artist) // ValueJoiner
            )

            // Log the join
            .peek((key, value)
                -> log.info("EventArtist with eventId '{}' and artistId '{}' registered with value '{}'",
                value.event.id(), value.artist.id(), value))

            // Rekey again by eventid and mark for repartition
            .selectKey((key, eventArtist)
                -> eventArtist.event.id(), Named.as("rekey-event-artist-by-eventid"))

            // Save it to a KTable. Causes a repartition
            .toTable(
                Materialized
                    .<String, EventArtist>as(persistentKeyValueStore(EVENT_ARTIST_KTABLE))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(EVENT_ARTIST_JSON_SERDE)
            );

        // Capture the backside of the table to log a confirmation that the Artist was received
        // Publish it to a stream for testing as well
        eventArtistKTable
            .toStream()
            .peek((key, eventArtist) -> log.info("EventArtist '{}' registered with value '{}'", key, eventArtist))
            .to(EVENT_ARTIST_KTABLE, Produced.with(Serdes.String(),EVENT_ARTIST_JSON_SERDE));


        // Get the Tickets stream, rekey by eventid, and join to the EventArtistsTable
        builder
            .stream(TICKET_INPUT_TOPIC, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
            .peek((ticketId, ticket) -> log.info("Ticket '{}' registered with value '{}'", ticketId, ticket))

            // Rekey by eventid and mark for repartition
            .selectKey((ticketId, ticket) -> ticket.eventid(), Named.as("rekey-ticket-by-eventid"))
            .peek((eventId, ticket) -> log.info("Ticket with eventId '{}' registered with value '{}'", eventId, ticket))

            // Join to EventArtist. Causes a repartition
            .join(
                eventArtistKTable, // Join to table
                (ticket, eventArtist) // Left value, right value
                    -> new EnrichedTicket(ticket, eventArtist.event, eventArtist.artist) // ValueJoiner
            )
            .peek((eventId, enrichedTicket) -> log.info("Enriched ticket joined with id '{}' and value '{}'", eventId, enrichedTicket))

            // Rekey and repartition by event.venueid
            .groupBy(
                (eventId, enrichedTicket) -> enrichedTicket.event.venueid(),
                Grouped.with(Serdes.String(), ENRICHED_TICKET_JSON_SERDE)
            )

            // Aggregate to order genre count by venue using a SortedCounterMap
            .aggregate(
                // Initializer
                SortedCounterMap::new,

                // Aggregator
                (venueId, enrichedTicket, venueGenreCounts) -> {
                    venueGenreCounts.incrementCount(enrichedTicket.artist.genre());
                    return venueGenreCounts;
                },

                // Save the mappings to a KTable
                Materialized
                    .<String, SortedCounterMap>as(persistentKeyValueStore("venue-genre-ticket-counts"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(COUNTER_MAP_JSON_SERDE)
                )

            // Turn it back into a stream to produce it to the output topic
            .toStream()
            .peek((venueId, sortedCounterMap) -> log.info("Aggregate sorted counter map created with venueId '{}' and value '{}'", venueId, sortedCounterMap))

            // Trim to only the top 3
            .mapValues(sortedCounterMap -> sortedCounterMap.top(3))
            .peek((key, counterMap) -> log.info("Venue {}'s 3 Top Selling Genres: {}", key, counterMap))

            // Output to the output topic
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), LINKED_HASH_MAP_JSON_SERDE));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EventArtist {
        public Event event;
        public Artist artist;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EnrichedTicket {
        public Ticket ticket;
        public Event event;
        public Artist artist;
    }

}