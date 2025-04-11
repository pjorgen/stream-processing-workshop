package org.improving.workshop.phase3;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;

import org.improving.workshop.samples.TopCustomerArtists.SortedCounterMap;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.venue.Venue;

import java.util.LinkedHashMap;

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

    public static final JsonSerde<TicketWithCustomerAndVenueAndState> TICKET_CUSTOMER_JSON_SERDE
        = new JsonSerde<>(TicketWithCustomerAndVenueAndState.class);
    public static final JsonSerde<TicketWithCustomerAddress> TICKET_WITH_CUSTOMER_ADDRESS_SERDE
        = new JsonSerde<>(TicketWithCustomerAddress.class);
    public static final JsonSerde<VenueWithState> VENUE_WITH_STATE_SERDE
        = new JsonSerde<>(VenueWithState.class);
    public static final JsonSerde<TicketWithCustomerAndVenue> TICKET_WITH_CUSTOMER_AND_VENUE_SERDE
        = new JsonSerde<>(TicketWithCustomerAndVenue.class);

    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-out-of-state-sales-ratio";

    public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE
        = new JsonSerde<>(SortedCounterMap.class);
    public static final JsonSerde<OutOfStateTicketSales> OUT_OF_STATE_JSON_SERDE
        = new JsonSerde<>(OutOfStateTicketSales.class);

    // Jackson is converting Value into Integer Not Long due to erasure,
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
        //Create a KTable for the events
        KTable<String, Event> eventsTable = builder
            .table(
                INPUT_TOPIC_EVENT,
                Materialized
                    .<String, Event>as(persistentKeyValueStore("events"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Streams.SERDE_EVENT_JSON)
            );

        //Create a KTable for the addresses
        KTable<String, Address> addressTable = builder
            .table(
                INPUT_TOPIC_ADDRESS,
                Materialized
                    .<String, Address>as(persistentKeyValueStore("addresses"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Streams.SERDE_ADDRESS_JSON)
            );

        //Create a KStream for the venues
        KStream<String, Venue> venueStream = builder
            .stream(
                INPUT_TOPIC_VENUE,
                Consumed.with(Serdes.String(), SERDE_VENUE_JSON)

            )
            .peek((venueId, venue) -> log.info("Venue ID: {} with Venue: {}", venueId, venue))

                .selectKey((venueId, venue) -> venue.addressid(), Named.as("rekey-by-addressid")
            );

        KTable<String, VenueWithState> venueWithStateTable = venueStream
            .join(
                addressTable,
                VenueWithState::new)
            .peek((addressId, venueWithState)
                -> log.info("Address ID: {} with Venue With State: {}", addressId, venueWithState))
            .<String>selectKey((addressId, venueWithState)
                -> venueWithState.venue.id(), Named.as("rekey-by-venueid"))
            .toTable(
                Materialized
                    .<String, VenueWithState, KeyValueStore<Bytes, byte[]>>
                        as("venue-with-state-table")
                .withValueSerde(VENUE_WITH_STATE_SERDE)
            );

        KTable<String, Address> customerAddressTable = addressTable
            .toStream()

            .<String>selectKey((addressId, address)
                -> address.customerid(), Named.as("rekey-by-customerid-from-address-table"))

            .toTable(
                Materialized
                    .<String, Address, KeyValueStore<Bytes, byte[]>>
                        as("customer-address-table")
                    .withValueSerde(SERDE_ADDRESS_JSON)
        );
        
        builder
            .stream(INPUT_TOPIC_TICKET, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))

            .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))

            // rekey by customerid so we can join against the address ktable
            .<String>selectKey((ticketId, ticketRequest)
                -> ticketRequest.customerid(), Named.as("rekey-by-customerid"))

            .<Address, TicketWithCustomerAddress>join(
                customerAddressTable,
                (customerId, ticket, address) -> new TicketWithCustomerAddress(ticket, address)
            )

            .peek((customerId, ticketWithCustomerAddress)
                -> log.info("Customer ID: {} with Ticket With Customer Address: {}", customerId, ticketWithCustomerAddress))

            // rekey by eventid so we can join against the event ktable

            .<String>selectKey((customerId, ticketWithCustomerAddress)
                -> ticketWithCustomerAddress.ticket.eventid(), Named.as("rekey-by-eventid"))

            .<Event, TicketWithCustomerAndVenue>join(
                eventsTable,
                (eventId, ticketWithCustomerAddress, event) -> 
                new TicketWithCustomerAndVenue(ticketWithCustomerAddress, event),
                Joined.<String, TicketWithCustomerAddress, Event>
                    with(Serdes.String(), TICKET_WITH_CUSTOMER_ADDRESS_SERDE, SERDE_EVENT_JSON)
            )

            .peek((customerId, ticketWithCustomerAndVenue)
                -> log.info("Customer ID: {} with Ticket With Customer And Venue: {}", customerId, ticketWithCustomerAndVenue))

            // rekey by venueid so we can join against the venue-with-state-table
            .<String>selectKey((customerId, ticketWithCustomerAndVenue)
                -> ticketWithCustomerAndVenue.event.venueid(), Named.as("rekey-by-venueid-for-join"))

            .<VenueWithState, TicketWithCustomerAndVenueAndState>join(
                venueWithStateTable,
                (venueId, ticketWithCustomerAndVenue, venueWithState) -> 
                new TicketWithCustomerAndVenueAndState(ticketWithCustomerAndVenue, venueWithState),
                Joined.<String, TicketWithCustomerAndVenue, VenueWithState>
                    with(Serdes.String(), TICKET_WITH_CUSTOMER_AND_VENUE_SERDE, VENUE_WITH_STATE_SERDE)
            )

            .peek((venueId, ticketWithCustomerAndVenueAndState)
                -> log.info("Ticket With Customer And Venue And State: {}", ticketWithCustomerAndVenueAndState))

            .groupByKey(Grouped.with(Serdes.String(), TICKET_CUSTOMER_JSON_SERDE))

            .aggregate(
                //initializer
                OutOfStateTicketSales::new,

                //Aggregate customer with out of state ticket sales
                (String venueId,
                 TicketWithCustomerAndVenueAndState ticketWithCustomerAndVenueAndState,
                 OutOfStateTicketSales outOfStateTicketSales) -> {
                    if (!outOfStateTicketSales.initialized) {
                        outOfStateTicketSales.initialize(ticketWithCustomerAndVenueAndState.venueWithState.venue);
                    }
                    outOfStateTicketSales.incrementTotalTicket();
                    String venueState = ticketWithCustomerAndVenueAndState.venueWithState.address.state();
                    String customerState = ticketWithCustomerAndVenueAndState.ticketWithCustomerAndVenue.ticketWithCustomerAddress.address.state();
                    if (!venueState.equals(customerState)) {
                        outOfStateTicketSales.incrementOutOfStateSale();
                    }
                    return outOfStateTicketSales;
                },

                //Materializing out of state sales to a ktable
                Materialized
                    .<String, OutOfStateTicketSales>as(persistentKeyValueStore("out-of-state-sales-counts"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(OUT_OF_STATE_JSON_SERDE)
            )

            .toStream()

            .peek((venueId, outOfStateSales)
                -> log.info("Venue ID: {} with Out Of State Sales: {}", venueId, outOfStateSales))

            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), OUT_OF_STATE_JSON_SERDE));
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TicketWithCustomerAddress {
        public Ticket ticket;
        public Address address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TicketWithCustomerAndVenue {
        public TicketWithCustomerAddress ticketWithCustomerAddress;
        public Event event;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VenueWithState {
        public Venue venue;
        public Address address;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TicketWithCustomerAndVenueAndState {
        public TicketWithCustomerAndVenue ticketWithCustomerAndVenue;
        public VenueWithState venueWithState;
    }

    @Data
    @AllArgsConstructor
    public static class OutOfStateTicketSales {
        private boolean initialized;
        private Venue venue; //Venue tied to the out of state ticket sales
        private double outOfStateTicket; //Number of out of state tickets sold
        private double totalTicket; //Total number of tickets sold

        public OutOfStateTicketSales() {
            initialized = false;
        }

        public void initialize(Venue venue) {
            this.venue = venue;
        }

        public void incrementOutOfStateSale() {
            this.outOfStateTicket++;
        }

        public void incrementTotalTicket() {
            this.totalTicket++;
        }

        public double getOutOfStateTicket() {
            return outOfStateTicket;
        }

        public double getOutOfStateTicketRatio() {
            double ratio = outOfStateTicket / totalTicket; //Ratio of out of state tickets to total tickets
            return Math.round(ratio * 100.0) / 100.0; //Round to 2 decimal places
        }
    }
}