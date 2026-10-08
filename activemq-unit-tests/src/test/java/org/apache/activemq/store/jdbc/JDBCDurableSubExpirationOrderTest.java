/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.jdbc.adapter.H2JDBCAdapter;
import org.apache.activemq.store.jdbc.h2.H2DB;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * The JDBC store acks a durable subscription by moving its last acked id, so expiring a message
 * acks every earlier message of that subscription too. Expiring a message that follows a
 * non-expired one must not make the non-expired message disappear, and expiring a message the
 * subscription has already consumed must not move its last acked id back, which would deliver
 * the later consumed messages again.
 */
@Category(ParallelTest.class)
public class JDBCDurableSubExpirationOrderTest {

    @Rule
    public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    private BrokerService broker;
    private final ActiveMQTopic topic = new ActiveMQTopic("test.topic");

    private void startBroker(boolean deleteAllMessages) throws Exception {
        startBroker(deleteAllMessages, 200);
    }

    private void startBroker(boolean deleteAllMessages, long expireMessagesPeriod) throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setSchedulerSupport(false);
        broker.setDataDirectoryFile(dataFileDir.getRoot());
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        jdbc.setDataSource(H2DB.createDataSource("JDBCDurableSubExpirationOrderTest"));
        jdbc.setAdapter(new H2JDBCAdapter());
        broker.setPersistenceAdapter(jdbc);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setExpireMessagesPeriod(expireMessagesPeriod);
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private Connection connect() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setClientID("clientId");
        Connection connection = factory.createConnection();
        connection.start();
        return connection;
    }

    @Test
    public void testExpiredMessageDoesNotAckEarlierMessage() throws Exception {
        startBroker(true);

        Connection connection = connect();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "sub1").close();

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("no-ttl"), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 0);
        producer.send(session.createTextMessage("ttl"), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 500);
        producer.send(session.createTextMessage("no-ttl-2"), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 0);

        // let the expiry task run several times after the second message expired
        Thread.sleep(1500);
        assertEquals(0, broker.getDestination(topic).getDestinationStatistics().getExpired().getCount());
        connection.close();

        // restart so the subscription is recovered from the database and not from a cursor in memory
        broker.stop();
        broker.waitUntilStopped();
        startBroker(false);

        connection = connect();
        try {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, "sub1");
            TextMessage received = (TextMessage) subscriber.receive(5000);
            assertNotNull("message without ttl was lost", received);
            assertEquals("no-ttl", received.getText());
            received = (TextMessage) subscriber.receive(5000);
            assertNotNull("second message without ttl was lost", received);
            assertEquals("no-ttl-2", received.getText());
            assertNull(subscriber.receive(500));
        } finally {
            connection.close();
        }
    }

    // browsing the topic (e.g. over JMX) expires messages too, it must not ack the earlier message either
    @Test
    public void testBrowseDoesNotAckEarlierMessage() throws Exception {
        // no expiry task, only the browse can expire messages
        startBroker(true, 0);

        Connection connection = connect();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "sub1").close();

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("no-ttl"), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 0);
        producer.send(session.createTextMessage("ttl"), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 500);
        Thread.sleep(1000);

        broker.getDestination(topic).browse();
        assertEquals(0, broker.getDestination(topic).getDestinationStatistics().getExpired().getCount());
        connection.close();

        broker.stop();
        broker.waitUntilStopped();
        startBroker(false, 0);

        connection = connect();
        try {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, "sub1");
            TextMessage received = (TextMessage) subscriber.receive(5000);
            assertNotNull("message without ttl was lost", received);
            assertEquals("no-ttl", received.getText());
        } finally {
            connection.close();
        }
    }

    @Test
    public void testBrowseExpiresMessages() throws Exception {
        startBroker(true, 0);

        Connection connection = connect();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "sub1").close();

            MessageProducer producer = session.createProducer(topic);
            for (int i = 0; i < 20; i++) {
                producer.send(session.createTextMessage("ttl" + i), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 500);
            }
            Thread.sleep(1000);

            broker.getDestination(topic).browse();
            assertEquals(20, broker.getDestination(topic).getDestinationStatistics().getExpired().getCount());
        } finally {
            connection.close();
        }
    }

    // all pending messages expired: the expiry task removes them and they are never delivered
    @Test
    public void testExpiredMessagesAreRemoved() throws Exception {
        startBroker(true);

        Connection connection = connect();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "sub1").close();

            MessageProducer producer = session.createProducer(topic);
            for (int i = 0; i < 20; i++) {
                producer.send(session.createTextMessage("ttl" + i), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 500);
            }

            assertTrue(Wait.waitFor(() ->
                broker.getDestination(topic).getDestinationStatistics().getExpired().getCount() == 20, 10000, 100));

            TopicSubscriber subscriber = session.createDurableSubscriber(topic, "sub1");
            assertNull(subscriber.receive(500));
        } finally {
            connection.close();
        }
    }

    // A message the subscription has already consumed stays in the table until the cleanup deletes
    // it (for non-prioritized messages, every 10th cleanup period). When it expires while the
    // subscription is offline, the expiry task must not ack it again: that would move the last
    // acked id back, and the later message, consumed too, would be delivered again after a restart.
    @Test
    public void testExpiredConsumedMessageIsNotDeliveredAgain() throws Exception {
        startBroker(true);
        consumeTtlThenNoTtl();

        // let the expiry task run several times after the consumed message expired
        Thread.sleep(2500);
        long expired = broker.getDestination(topic).getDestinationStatistics().getExpired().getCount();

        TextMessage again = restartAndReceive(200);
        assertNull("consumed message delivered again: " + (again == null ? "" : again.getText()), again);
        assertEquals("consumed message counted as expired", 0, expired);
    }

    // browsing the topic (e.g. over JMX) must not ack a consumed message again either
    @Test
    public void testBrowseDoesNotDeliverConsumedMessageAgain() throws Exception {
        startBroker(true, 0);
        consumeTtlThenNoTtl();
        Thread.sleep(1500);

        broker.getDestination(topic).browse();
        long expired = broker.getDestination(topic).getDestinationStatistics().getExpired().getCount();

        TextMessage again = restartAndReceive(0);
        assertNull("consumed message delivered again: " + (again == null ? "" : again.getText()), again);
        assertEquals("consumed message counted as expired", 0, expired);
    }

    // the subscription consumes a message with a 1 s TTL and a later one without, then goes offline
    private void consumeTtlThenNoTtl() throws Exception {
        Connection connection = connect();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, "sub1");
            MessageProducer producer = session.createProducer(topic);
            producer.send(session.createTextMessage("ttl"), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 1000);
            producer.send(session.createTextMessage("no-ttl"), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 0);
            TextMessage received = (TextMessage) subscriber.receive(5000);
            assertNotNull(received);
            assertEquals("ttl", received.getText());
            received = (TextMessage) subscriber.receive(5000);
            assertNotNull(received);
            assertEquals("no-ttl", received.getText());
        } finally {
            connection.close();
        }
    }

    // restart so the subscription is recovered from the database, then reconnect it
    private TextMessage restartAndReceive(long expireMessagesPeriod) throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        startBroker(false, expireMessagesPeriod);

        Connection connection = connect();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, "sub1");
            return (TextMessage) subscriber.receive(2000);
        } finally {
            connection.close();
        }
    }
}
