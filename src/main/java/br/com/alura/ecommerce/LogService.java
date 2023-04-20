package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var logService = new LogService();
        try (var service = new KafkaService(
                LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"), // Se inscreve em TODOS os tópicos
                logService::parse,
                Order.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
            }
        }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("----------------------------------------------------------------------------------------------------");
        System.out.println("Sucesso ao consumir de " + record.topic() + " ::: [partition: " + record.partition() + "]" +
                "[offset: " + record.offset() + "][key: " + record.key() + "][value: " + record.value() + "]" +
                "[timestamp: " + record.timestamp() + "]");
    }
}
