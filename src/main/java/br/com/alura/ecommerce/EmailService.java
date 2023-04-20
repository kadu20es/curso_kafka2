package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService<Order>(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", emailService::parse, Order.class, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {

        System.out.println("Sending e-mail");
        System.out.println("Sucesso ao consumir de "
                + record.topic()
                + " ::: [partition: " + record.partition() + "]"
                + "[offset: " + record.offset() + "]"
                + "[key: " + record.key() + "]"
                + "[value: " + record.value() + "]"
                + "[timestamp: " + record.timestamp() + "]");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("E-mail sent");
    }

}
