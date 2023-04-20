package br.com.alura.ecommerce;

import java.math.BigDecimal;

public class Order {
    private final String userID, orderId;
    private final BigDecimal amount;

    public Order(String userID, String orderId, BigDecimal amount) {
        this.userID = userID;
        this.orderId = orderId;
        this.amount = amount;
    }
}
