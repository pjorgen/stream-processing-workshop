package org.improving.workshop.phase3;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.venue.Venue;
import org.msse.demo.mockdata.customer.profile.Customer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.improving.workshop.utils.DataFaker;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.improving.workshop.samples.TopCustomerArtists.SortedCounterMap;
import org.springframework.kafka.support.serializer.JsonSerde;

class OutOfStateSalesTest {
    private TopologyTestDriver driver;

    private TestInputTopic<String, Ticket> ticketInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Venue> venueInputTopic;
    // private TestInputTopic<String, Customer> customerInputTopic;
    private TestOutputTopic<String, OutOfStateSales.OutOfStateTicketSales> outputTopic;

    @BeforeEach
    public void setup() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        OutOfStateSales.configureTopology(streamsBuilder);

        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        ticketInputTopic = driver.createInputTopic(
                OutOfStateSales.INPUT_TOPIC_TICKET,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );

        // customerInputTopic = driver.createInputTopic(
        //         Streams.TOPIC_DATA_DEMO_CUSTOMERS,
        //         Serdes.String().serializer(),
        //         Streams.SERDE_CUSTOMER_JSON.serializer()
        // );

        addressInputTopic = driver.createInputTopic(
                OutOfStateSales.INPUT_TOPIC_ADDRESS,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        );

        eventInputTopic = driver.createInputTopic(
                OutOfStateSales.INPUT_TOPIC_EVENT,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        venueInputTopic = driver.createInputTopic(
                OutOfStateSales.INPUT_TOPIC_VENUE,
                Serdes.String().serializer(),
                new JsonSerde<>(Venue.class).serializer()
        );

        outputTopic = driver.createOutputTopic(
                OutOfStateSales.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                OutOfStateSales.OUT_OF_STATE_JSON_SERDE.deserializer()
        );
    }

   @AfterEach
   public void cleanup() {
       if (driver != null) {
           driver.close();
       }
   }

    @Test
    @DisplayName("Out of state sales by venue")
    public void outOfStateSalesByVenue() {
        // ARRANGE
        String eventId = "event-1";
        String venueId = "venue-1";
        String customerId = "customer-1";
        String addressId1 = "address-1";
        String addressId2 = "address-2";
        
        
        // ACT
        Event event = new Event(eventId, "artist-1", venueId, 5, "today");
        eventInputTopic.pipeInput(eventId, event);

        Address address1 = new Address(
            addressId1, customerId, "cd", "HOME", "111 1st St", "Apt 2",
            "Madison", "WI", "55555", "1234", "USA", 0.0, 0.0);
        addressInputTopic.pipeInput(addressId1, address1);

        Address address2 = new Address(
            addressId2, "cust-678", "cd", "BUSINESS", "123 31st St", " ",
            "Minneapolis", "MN", "55414", "1234", "USA", 0.0, 0.0);
        addressInputTopic.pipeInput(addressId2, address2);

        Venue venue = new Venue(venueId, addressId2, "Test Venue", 500);
        venueInputTopic.pipeInput(venueId, venue);

        Ticket ticket = DataFaker.TICKETS.generate(customerId, eventId);
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket);

        // ASSERT
        var outputRecords = outputTopic.readRecordsToList();
        assertEquals(1, outputRecords.size());
    }
}