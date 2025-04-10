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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;

import org.improving.workshop.samples.TopCustomerArtists.SortedCounterMap;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.venue.Venue;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;
import org.improving.workshop.Streams;

@Slf4j
public class OutOfStateSales {
    // Reference TOPIC_DATA_DEMO_* properties in Streams
    public static final String INPUT_TOPIC_TICKET = TOPIC_DATA_DEMO_TICKETS;
    public static final String INPUT_TOPIC_ADDRESS = TOPIC_DATA_DEMO_ADDRESSES;
    public static final String INPUT_TOPIC_EVENT = TOPIC_DATA_DEMO_EVENTS;
    public static final String INPUT_TOPIC_VENUE = TOPIC_DATA_DEMO_VENUES;

    // public static final JsonSerde<Address> SERDE_ADDRESS_JSON = new JsonSerde<>(Address.class);
    // public static final JsonSerde<Event> SERDE_EVENT_JSON = new JsonSerde<>(Event.class);
    // public static final JsonSerde<Venue> SERDE_VENUE_JSON = new JsonSerde<>(Venue.class);
    public static final JsonSerde<TicketWithCustomerAndVenueAndState> TICKET_CUSTOMER_JSON_SERDE = new JsonSerde<>(TicketWithCustomerAndVenueAndState.class);


    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-out-of-state-sales-ratio";

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

        KTable<String, Event> eventsTable = builder
                .table(
                        INPUT_TOPIC_EVENT,
                        Materialized
                            .<String, Event>as(persistentKeyValueStore("events"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Streams.SERDE_EVENT_JSON)
                            
                );

        KTable<String, Address> addressTable = builder
                .table(
                        INPUT_TOPIC_ADDRESS,
                        Materialized
                            .<String, Address>as(persistentKeyValueStore("addresses"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Streams.SERDE_ADDRESS_JSON)
                            
                );

        KTable<String, VenueWithState> venueWithStateTable = builder
                .stream(INPUT_TOPIC_VENUE, Consumed.with(Serdes.String(), SERDE_VENUE_JSON))
                .<String>selectKey((venueId, venue) -> venue.addressid(), Named.as("rekey-by-addressid"))
                .join(
                    addressTable,
                    (addressId, venue, address) -> new VenueWithState(venue, address)
                )
                .peek((venueId, venueWithState) -> log.info("Venue ID: {} with Venue With State: {}", venueId, venueWithState))
                .<String>selectKey((addressId, venueWithState) -> venueWithState.venue.id(), Named.as("rekey-by-venueid"))
                .toTable(Materialized.as("venue-with-state-table"));
        
        builder
            .stream(INPUT_TOPIC_TICKET, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
            .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))
            // rekey by customerid so we can join against the address ktable
            .<String>selectKey((ticketId, ticketRequest) -> ticketRequest.customerid(), Named.as("rekey-by-customerid"))
            .<Address, TicketWithCustomerAddress>join(
                addressTable,
                (customerId, ticket, address) -> new TicketWithCustomerAddress(ticket, address)
            )
            .peek((customerId, ticketWithCustomerAddress) -> log.info("Customer ID: {} with Ticket With Customer Address: {}", customerId, ticketWithCustomerAddress))
            // rekey by eventid so we can join against the event ktable
            .<String>selectKey((customerId, ticketWithCustomerAddress) -> ticketWithCustomerAddress.ticket().eventid(), Named.as("rekey-by-eventid"))
            .<Event, TicketWithCustomerAndVenue>join(
                eventsTable,
                (eventId, ticketWithCustomerAddress, event) -> new TicketWithCustomerAndVenue(ticketWithCustomerAddress, event)
            )
            .peek((customerId, ticketWithCustomerAndVenue) -> log.info("Customer ID: {} with Ticket With Customer And Venue: {}", customerId, ticketWithCustomerAndVenue))
            // rekey by venueid so we can join against the venue-with-state-table
            .<String>selectKey((customerId, ticketWithCustomerAndVenue) -> ticketWithCustomerAndVenue.event.venueid(), Named.as("rekey-by-venueid-for-join"))
            .<VenueWithState, TicketWithCustomerAndVenueAndState>join(
                venueWithStateTable,
                (ticketWithCustomerAndVenue, venueWithState) -> new TicketWithCustomerAndVenueAndState(ticketWithCustomerAndVenue, venueWithState)
            )
            .peek((venueId, ticketWithCustomerAndVenueAndState) -> log.info("Ticket With Customer And Venue And State: {}", ticketWithCustomerAndVenueAndState))
            .groupByKey(Grouped.with(Serdes.String(), TICKET_CUSTOMER_JSON_SERDE))
            .aggregate(
                //initializer
                SortedCounterMap::new,

                //Aggregate customer with out of state ticket sales
                (String venueId, TicketWithCustomerAndVenueAndState ticketWithCustomerAndVenueAndState, SortedCounterMap outOfStateSales) -> {
                    String venueState = ticketWithCustomerAndVenueAndState.venueWithState.address.state();
                    String customerState = ticketWithCustomerAndVenueAndState.ticketWithCustomerAndVenue.ticketWithCustomerAddress.address().state();
                    if (!venueState.equals(customerState)) {
                        outOfStateSales.incrementCount(venueId);
                    }
                    return outOfStateSales;
                },

                //Materializing out of state sales to a ktable
                Materialized
                    .<String, SortedCounterMap>as(persistentKeyValueStore("out-of-state-sales-counts"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(COUNTER_MAP_JSON_SERDE)
            )
            .toStream()
            .peek((venueId, outOfStateSales) -> log.info("Venue ID: {} with Out Of State Sales: {}", venueId, outOfStateSales))


            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), COUNTER_MAP_JSON_SERDE));
    }



    @Data
    public static class TicketWithCustomerAddress {
        private org.msse.demo.mockdata.music.ticket.Ticket ticket;
        private Address address;

        public TicketWithCustomerAddress(org.msse.demo.mockdata.music.ticket.Ticket ticket, Address address) {
            this.ticket = ticket;
            this.address = address;
        }

        public org.msse.demo.mockdata.music.ticket.Ticket ticket() {
            return ticket;
        }

        public Address address() {
            return address;
        }
    }

    @Data
    @AllArgsConstructor
    public static class TicketWithCustomerAndVenue {
        private TicketWithCustomerAddress ticketWithCustomerAddress;
        private Event event;

        public org.msse.demo.mockdata.music.ticket.Ticket ticket() {
            return ticketWithCustomerAddress.ticket();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VenueWithState {
        public Venue venue;
        public Address address;

        // public Venue venue() {
        //     return venue;
        // }

        // public Address address() {
        //     return address;
        // }
    }


    @Data
    @AllArgsConstructor
    public static class TicketWithCustomerAndVenueAndState {
        private TicketWithCustomerAndVenue ticketWithCustomerAndVenue;
        private VenueWithState venueWithState;
    }

}