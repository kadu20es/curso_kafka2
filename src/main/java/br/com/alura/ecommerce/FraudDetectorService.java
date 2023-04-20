package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;


public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudDetectorService::parse, Order.class, new HashMap<>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {

        System.out.println("Processing new order. Checking for fraud");
        System.out.println("Sucesso ao consumir de " + record.topic()
                + " ::: [partition: " + record.partition() + "]"
                + "[offset: " + record.offset() + "]"
                + "[key: " + record.key() + "]"
                + "[value: " + record.value() + "]"
                + "[timestamp: " + record.timestamp() + "]");

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Order processed");

    }

}
