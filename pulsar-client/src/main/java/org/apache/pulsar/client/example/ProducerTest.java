package org.apache.pulsar.client.example;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class ProducerTest {
    public static void main(String[] args) {
        try {
            PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
            Producer<byte[]> producer = client.newProducer().topic("persistent://my-tenant/my-ns/my-topic").create();
            producer.send("hello".getBytes());
            producer.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

    }
}
