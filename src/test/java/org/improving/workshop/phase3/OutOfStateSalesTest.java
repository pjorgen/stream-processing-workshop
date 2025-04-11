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
import org.improving.workshop.phase3.OutOfStateSales;

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
    @DisplayName("One out of state sale by venue")
    public void oneOutOfStateSale() {
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
        assertEquals(1, outputRecords.get(0).value().getOutOfStateTicket());
    }

    @Test
    @DisplayName("2 out of state sales by venue")
    public void twoOutOfStateSalesByVenue() {
        // ARRANGE
        String eventId = "event-1";
        String venueId = "venue-1";
        String customerId1 = "customer-1";
        String customerId2 = "customer-2";
        String addressId1 = "address-1";
        String addressId2 = "address-2";
        String addressId3 = "address-3";
        
        // ACT
        Event event = new Event(eventId, "artist-1", venueId, 5, "today");
        eventInputTopic.pipeInput(eventId, event);

        Address address1 = new Address(
            addressId1, customerId1, "cd", "HOME", "111 1st St", "Apt 2",
            "Madison", "WI", "55444", "1234", "USA", 0.0, 0.0);
        addressInputTopic.pipeInput(addressId1, address1);

        Address address2 = new Address(
            addressId2, "cust-678", "cd", "BUSINESS", "123 31st St", " ",
            "Minneapolis", "MN", "55414", "1234", "USA", 0.0, 0.0);
        addressInputTopic.pipeInput(addressId2, address2);

        Address address3 =  new Address(
            addressId3, customerId2, "TD", "HOME", "333 3rd St", "Apt 4",
            "New York", "NY", "10002", "9012", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId3, address3);

        Venue venue = new Venue(venueId, addressId2, "Test Venue", 500);
        venueInputTopic.pipeInput(venueId, venue);

        Ticket ticket = DataFaker.TICKETS.generate(customerId1, eventId);
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket);

        Ticket ticket2 = DataFaker.TICKETS.generate(customerId2, eventId);
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket2);

        // ASSERT
        var outputRecords = outputTopic.readRecordsToList();
        assertEquals(1, outputRecords.get(0).value().getOutOfStateTicket());
        assertEquals(2, outputRecords.get(1).value().getOutOfStateTicket());
    }

    @Test
    @DisplayName("No out of state sale by venue")
    public void noOutOfStateSale() {
        // ARRANGE
        String eventId = "event-1";
        String venueId = "venue-1";
        String customerId = "customer-1";
        String addressId1 = "address-1";
        String addressId2 = "address-2";
        
        
        // ACT
        Event event = new Event(eventId, "artist-1", venueId, 5, "today");
        eventInputTopic.pipeInput(eventId, event);

        //Customer Address is in state
        Address address1 = new Address(
            addressId1, customerId, "cd", "HOME", "123 17th St", " ",
            "Minneapolis", "MN", "55444", "1234", "USA", 0.0, 0.0);
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
        assertEquals(0, outputRecords.get(0).value().getOutOfStateTicket());
    }

    @Test
    @DisplayName("A mix of out of state and in state sales by venue")
    public void mixOfOutOfStateAndInStateSales() {
        // ARRANGE
        String eventId = "event-77";
        String venueId = "venue-33";

        String customerId = "customer-1";
        String customerId2 = "customer-2";
        String customerId3 = "customer-3";
        String customerId4 = "customer-4";

        String addressId1 = "address-555";  // In-state (CA)
        String addressId2 = "address-666";  // Out-of-state (NY)
        String addressId3 = "address-777";  // Out-of-state (NY)
        String addressId4 = "address-888";  // In-state (CA)
        String addressId5 = "address-44";  // Venue Address (CA)

        
        // ACT
        Event event = new Event(eventId, "artist-1", venueId, 5, "today");
        eventInputTopic.pipeInput(eventId, event);

        Address address1 = new Address(
            addressId1, customerId, "TD", "HOME", "111 1st St", "Apt 2",
            "Los Angeles", "CA", "90001", "1234", "USA", 34.0522, -118.2437);
        addressInputTopic.pipeInput(addressId1, address1);

        Address address2 =  new Address(
            addressId2, customerId2, "TD", "HOME", "222 2nd St", "Apt 3",
            "New York", "NY", "10001", "5678", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId2, address2);

        Address address3 = new Address(
            addressId3, customerId3, "TD", "HOME", "333 3rd St", "Apt 4",
            "New York", "NY", "10002", "9012", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId3, address3);

        Address address4 = new Address(
            addressId4, customerId4, "TD", "HOME", "444 4th St", "Apt 5",
            "San Francisco", "CA", "94101", "3456", "USA", 37.7749, -122.4194);
        addressInputTopic.pipeInput(addressId4, address4);

        // Add venue address
        Address address5 = new Address(
            addressId5, "venue-owner", "TD", "BUSINESS", "555 5th St", "",
            "San Francisco", "CA", "94102", "7890", "USA", 37.7749, -122.4194);
        addressInputTopic.pipeInput(addressId5, address5);

        Venue venue = new Venue(venueId, addressId5, "Cali Venue", 5000);
        venueInputTopic.pipeInput(venueId, venue);

        Ticket ticket = DataFaker.TICKETS.generate(customerId, eventId); //In state ticket
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket);

        Ticket ticket2 = DataFaker.TICKETS.generate(customerId2, eventId); //Out of state ticket
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket2);

        Ticket ticket3 = DataFaker.TICKETS.generate(customerId3, eventId); //Out of state ticket
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket3);

        Ticket ticket4 = DataFaker.TICKETS.generate(customerId4, eventId); //In state ticket
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket4);


        // ASSERT
        var outputRecords = outputTopic.readRecordsToList();
        assertEquals(0, outputRecords.get(0).value().getOutOfStateTicket());
        assertEquals(1, outputRecords.get(1).value().getOutOfStateTicket());
        assertEquals(2, outputRecords.get(2).value().getOutOfStateTicket());
        assertEquals(2, outputRecords.get(3).value().getOutOfStateTicket());
    }

    @Test
    @DisplayName("Multiple venues with out of state sales")
    public void multipleVenuesWithOutOfStateSales() {
        // ARRANGE
        String eventId = "event-77";
        String eventId2 = "event-88";

        String venueId = "venue-33";
        String venueId2 = "venue-44";

        String customerId = "customer-1";
        String customerId2 = "customer-2";
        String customerId3 = "customer-3";
        String customerId4 = "customer-4";

        String addressId1 = "address-555";  // Out-of-state (CA)
        String addressId2 = "address-666";  // Out-of-state (NY)
        String addressId3 = "address-777";  // Out-of-state (NY)
        String addressId4 = "address-888";  // In-state (CA)
        String addressId5 = "address-44";  // Venue Address (CA)
        String addressId6 = "address-55";  // Venue Address (NY)
        
        // ACT
        Event event = new Event(eventId, "artist-1", venueId, 5, "today");
        eventInputTopic.pipeInput(eventId, event);

        Event event2 = new Event(eventId2, "artist-1", venueId2, 5, "today");
        eventInputTopic.pipeInput(eventId2, event2);

        Address address1 = new Address(
            addressId1, customerId, "TD", "HOME", "111 1st St", "Apt 2",
            "Los Angeles", "CA", "90001", "1234", "USA", 34.0522, -118.2437);
        addressInputTopic.pipeInput(addressId1, address1);

        Address address2 =  new Address(
            addressId2, customerId2, "TD", "HOME", "222 2nd St", "Apt 3",
            "New York", "NY", "10001", "5678", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId2, address2);

        Address address3 = new Address(
            addressId3, customerId3, "TD", "HOME", "333 3rd St", "Apt 4",
            "New York", "NY", "10002", "9012", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId3, address3);

        Address address4 = new Address(
            addressId4, customerId4, "TD", "HOME", "444 4th St", "Apt 5",
            "San Francisco", "CA", "94101", "3456", "USA", 37.7749, -122.4194);
        addressInputTopic.pipeInput(addressId4, address4);

        // Add venue address
        Address address5 = new Address(
            addressId5, "venue-owner", "TD", "BUSINESS", "555 5th St", "",
            "San Francisco", "CA", "94102", "7890", "USA", 37.7749, -122.4194);
        addressInputTopic.pipeInput(addressId5, address5);

        Address address6 = new Address(
            addressId6, "venue-owner", "TD", "BUSINESS", "666 6th St", "",
            "New York", "NY", "10001", "7890", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId6, address6);

        Venue venue = new Venue(venueId, addressId5, "Cali Venue", 5000);
        venueInputTopic.pipeInput(venueId, venue);

        Venue venue2 = new Venue(venueId2, addressId6, "NY Venue", 5000);
        venueInputTopic.pipeInput(venueId2, venue2);

        Ticket ticket = DataFaker.TICKETS.generate(customerId, eventId2); //Out of state ticket for venue 2
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket);

        Ticket ticket2 = DataFaker.TICKETS.generate(customerId2, eventId); //Out of state ticket for venue 1
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket2);

        Ticket ticket3 = DataFaker.TICKETS.generate(customerId3, eventId); //Out of state ticket for venue 1
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket3);

        Ticket ticket4 = DataFaker.TICKETS.generate(customerId4, eventId); //In state ticket for venue 1
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket4);


        // ASSERT
        var outputRecords = outputTopic.readRecordsToList();
        assertEquals(1, outputRecords.get(0).value().getOutOfStateTicket());
        assertEquals(1, outputRecords.get(1).value().getOutOfStateTicket());
        assertEquals(2, outputRecords.get(2).value().getOutOfStateTicket());
        assertEquals(2, outputRecords.get(3).value().getOutOfStateTicket());
    }

    @Test
    @DisplayName("Test out of state ticket ratio")
    public void testOutOfStateTicketRatio() {
        // ARRANGE
        String eventId = "event-1";

        String venueId = "venue-1";

        String customerId1 = "customer-1";
        String customerId2 = "customer-2";
        String customerId3 = "customer-3";
        String customerId4 = "customer-4";

        String addressId1 = "address-1";
        String addressId2 = "address-2"; //Venue Address
        String addressId3 = "address-3";
        String addressId4 = "address-4";
        String addressId5 = "address-5";

        // ACT
        Event event = new Event(eventId, "artist-1", venueId, 5, "today");
        eventInputTopic.pipeInput(eventId, event);

        Address address1 = new Address(
            addressId1, customerId1, "cd", "HOME", "111 1st St", "Apt 2",
            "Madison", "WI", "55444", "1234", "USA", 0.0, 0.0);
        addressInputTopic.pipeInput(addressId1, address1);

        Address address2 = new Address(
            addressId2, "cust-678", "cd", "BUSINESS", "123 31st St", " ",
            "Minneapolis", "MN", "55414", "1234", "USA", 0.0, 0.0);
        addressInputTopic.pipeInput(addressId2, address2);

        Address address3 =  new Address(
            addressId3, customerId2, "TD", "HOME", "333 3rd St", "Apt 4",
            "St. Paul", "MN", "55414", "1234", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId3, address3);

        Address address4 = new Address(
            addressId4, customerId3, "TD", "HOME", "444 4th St", "Apt 5",
            "St. Paul", "MN", "55414", "1234", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId4, address4);

        Address address5 = new Address(
            addressId5, customerId4, "TD", "HOME", "555 5th St", "",
            "St. Paul", "MN", "55414", "1234", "USA", 40.7128, -74.0060);
        addressInputTopic.pipeInput(addressId5, address5);

        Venue venue = new Venue(venueId, addressId2, "Test Venue", 500);
        venueInputTopic.pipeInput(venueId, venue);

        Ticket ticket = DataFaker.TICKETS.generate(customerId1, eventId);
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket); //Out of state ticket

        Ticket ticket2 = DataFaker.TICKETS.generate(customerId2, eventId);
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket2); //In state ticket

        Ticket ticket3 = DataFaker.TICKETS.generate(customerId3, eventId);
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket3); //In state ticket

        Ticket ticket4 = DataFaker.TICKETS.generate(customerId4, eventId);
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), ticket4); //In state ticket

        // ASSERT
        var outputRecords = outputTopic.readRecordsToList();
        //Out of state ticket ratio is should decline as more in state tickets are sold.
        assertEquals(1, outputRecords.get(0).value().getOutOfStateTicketRatio());
        assertEquals(0.5, outputRecords.get(1).value().getOutOfStateTicketRatio());
        assertEquals(0.33, outputRecords.get(2).value().getOutOfStateTicketRatio());
        assertEquals(0.25, outputRecords.get(3).value().getOutOfStateTicketRatio());
    }


}