package org.apache.pulsar.client.example;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class ConsumerTest {
    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar:///localhost:6650").build();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-tenant/my-ns/my-topic")
                .subscriptionName("my-subscription-name").subscribe();

        Message<byte[]> msg = null;

        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            System.out.println("Received: " + new String(msg.getData()));
        }
        consumer.acknowledge(msg);
        pulsarClient.close();
    }
}
