package org.improving.workshop.phase3;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.TopCustomerArtists.SortedCounterMap;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OutOfStateSalesTest {
    private TopologyTestDriver driver;

    // tjl
    private TestOutputTopic<String, Event> eventKTableOutputTopic;
    private TestOutputTopic<String, Address> addressKTableOutputTopic;

    private TestInputTopic<String, Ticket> ticketInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Venue> venueInputTopic;
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestOutputTopic<String, SortedCounterMap> outputTopic;

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

        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                Serdes.String().serializer(),
                Streams.SERDE_CUSTOMER_JSON.serializer()
        );

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

        //tjl
        eventKTableOutputTopic = driver.createOutputTopic(
                OutOfStateSales.EVENT_KTABLE_TOPIC,
                Serdes.String().deserializer(),
                OutOfStateSales.EVENT_JSON_SERDE.deserializer()
        );

        addressKTableOutputTopic = driver.createOutputTopic(
                OutOfStateSales.ADDRESS_KTABLE_TOPIC,
                Serdes.String().deserializer(),
                OutOfStateSales.ADDRESS_JSON_SERDE.deserializer()
        );

        outputTopic = driver.createOutputTopic(
                OutOfStateSales.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                OutOfStateSales.COUNTER_MAP_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    @DisplayName("Test Event KTable")
    public void testEventKTable() {
        // ARRANGE
        String eventId = "event-1";
        String artistId = "artist-1";
        String venueId = "venue-1";
        int venueCap = 500;

        // ACT
        Event event = DataFaker.EVENTS.generate(eventId, artistId, venueId, venueCap);
        eventInputTopic.pipeInput(eventId, event);

        // ASSERT
        var outputRecords = eventKTableOutputTopic.readRecordsToList();
        assertEquals(1, outputRecords.size());
        assertEquals(eventId, outputRecords.get(0).getKey());
        assertEquals(artistId, outputRecords.get(0).getValue().artistid());
        assertEquals(venueId, outputRecords.get(0).getValue().venueid());
    }

    @Test
    @DisplayName("Test Address KTable")
    public void testAddressKTable() {
        // ARRANGE
        String addressId = "address-test-1";
        String customerId = "customer-test-1";

        // ACT
        // Generate the address with DataFaker
        Address address = DataFaker.ADDRESSES.generateCustomerAddress(customerId);

        // Get the actual values from the generated address
        String actualCustomerId = address.customerid();
        String actualState = address.state();

        // Pipe it to the input topic
        addressInputTopic.pipeInput(addressId, address);

        // ASSERT
        var outputRecords = addressKTableOutputTopic.readRecordsToList();
        assertEquals(1, outputRecords.size());

        // Check that the output matches using the actual data
        assertEquals(actualCustomerId, outputRecords.get(0).getValue().customerid());
        assertEquals(actualState, outputRecords.get(0).getValue().state());
    }

}