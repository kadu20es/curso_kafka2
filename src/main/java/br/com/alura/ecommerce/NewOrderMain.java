package br.com.alura.ecommerce;


import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) { // se der sucesso ou sair por causa de uma exception, o dispatcher será fechado
            try (var emailDispatcher = new KafkaDispatcher<>()) {
        //var producer = new KafkaProducer<String, String>(properties()); // <chave, valor> --- cria o produtor
                for (int i =0; i < 10; i++) {

                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var orderAmount = new BigDecimal(Math.random() * 5000 + 1.99);
                    var subject = "You have a new order!";
                    var body = "Thank you for your order! We are processing your order";
                    var email = new Email(subject, body);

                    var order = new Order(userId, orderId, orderAmount);
                    //var email = "Thank you for your order! We are processing your order";
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, order);
                }
            }
        }
    }
}
