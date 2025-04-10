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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.improving.workshop.utils.DataFaker.STREAMS;
import static org.junit.jupiter.api.Assertions.*;

class OutOfStateSalesTest {
    private TopologyTestDriver driver;

    private TestInputTopic<String, Ticket> ticketInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Venue> venueInputTopic;
    private TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic;

    @BeforeEach
    public void setup() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        OutOfStateSales.configureTopology(streamsBuilder);

        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );

        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        );

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        venueInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_VENUES,
                Serdes.String().serializer(),
                Streams.SERDE_VENUE_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
                OutOfStateSales.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                OutOfStateSales.LINKED_HASH_MAP_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    public void cleanup() { driver.close(); }

    @Test
@DisplayName("out of state sales percentage test")
public void testOutOfStateSalesPercentage() {
    // Given a venue in California
    var venueId = "venue-33";
    var venue = new Venue(venueId, "address-44", "Cali Venue", 5000);
    venueInputTopic.pipeInput(venueId, venue);

    // And an event at that venue
    var eventId = "event-77";
    var event = new Event(eventId, "artist-22", venueId, 5000, "2024-03-20");
    eventInputTopic.pipeInput(eventId, event);

    // And multiple customer addresses (mix of in-state and out-of-state)
    var addressId1 = "address-555";  // In-state (CA)
    var addressId2 = "address-666";  // Out-of-state (NY)
    var addressId3 = "address-777";  // Out-of-state (NY)
    var addressId4 = "address-888";  // In-state (CA)
    
    var address1 = new Address(
            addressId1, "cust-678", "TD", "HOME", "111 1st St", "Apt 2",
            "Los Angeles", "CA", "90001", "1234", "USA", 34.0522, -118.2437);
    var address2 = new Address(
            addressId2, "cust-789", "TD", "HOME", "222 2nd St", "Apt 3",
            "New York", "NY", "10001", "5678", "USA", 40.7128, -74.0060);
    var address3 = new Address(
            addressId3, "cust-012", "TD", "HOME", "333 3rd St", "Apt 4",
            "New York", "NY", "10002", "9012", "USA", 40.7128, -74.0060);
    var address4 = new Address(
            addressId4, "cust-345", "TD", "HOME", "444 4th St", "Apt 5",
            "San Francisco", "CA", "94101", "3456", "USA", 37.7749, -122.4194);
    
    addressInputTopic.pipeInput(addressId1, address1);
    addressInputTopic.pipeInput(addressId2, address2);
    addressInputTopic.pipeInput(addressId3, address3);
    addressInputTopic.pipeInput(addressId4, address4);

    // And multiple purchased tickets for the event
    var ticketId1 = "ticket-111";
    var ticketId2 = "ticket-222";
    var ticketId3 = "ticket-333";
    var ticketId4 = "ticket-444";
    
    var ticket1 = new Ticket(ticketId1, "customer-1", eventId, 100.00);
    var ticket2 = new Ticket(ticketId2, "customer-2", eventId, 100.00);
    var ticket3 = new Ticket(ticketId3, "customer-3", eventId, 100.00);
    var ticket4 = new Ticket(ticketId4, "customer-4", eventId, 100.00);
    
    ticketInputTopic.pipeInput(ticketId1, ticket1);
    ticketInputTopic.pipeInput(ticketId2, ticket2);
    ticketInputTopic.pipeInput(ticketId3, ticket3);
    ticketInputTopic.pipeInput(ticketId4, ticket4);

    // When reading the output records
    var outputRecords = outputTopic.readRecordsToList();

    // Then the expected number of records were received
    assertEquals(1, outputRecords.size());

    // And the output contains venue, state, and percentage
    var salesRecord = outputRecords.getLast();
    assertEquals(venueId, salesRecord.key());
    
    var result = salesRecord.value();
    assertEquals(venue, result.get("venue"));
    assertEquals("NY", result.get("state"));
    assertNotNull(result.get("out-of-state-sales-percent"));
    assertInstanceOf(Long.class, result.get("out-of-state-sales-percent"));
    assertTrue(result.get("out-of-state-sales-percent") >= 0L && 
           result.get("out-of-state-sales-percent") <= 100L);
    }
}