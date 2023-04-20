package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, T> producer;

    KafkaDispatcher(){
        this.producer = new KafkaProducer<String, T>(properties());
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9090"); // configura o local de acesso ao kafka
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // converte a classe de string para bytes
        //properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // converte as mensagens string para bytes
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName()); // converte as mensagens string para bytes
        return properties;
    }

    void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value); // <chave, valor>(TOPICO, CHAVE, VALOR)

        Callback callback = (data, ex) -> { // o (data, ex) -> { é uma callback function que se tiver o erro, imprime o erro, se não tiver, imprime os dados do envio
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso ao enviar " + data.topic() + " ::: [partition: " + data.partition() + "] [offset: " + data.offset() + "][timestamp: " + data.timestamp() + "]");
        };

        producer.send(record, callback).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}
