package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<String>()) {
                var userEmail = Math.random() + "@email.com";
                for (var i = 0; i < 3; i++) {

                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);

                    var order = new Order(orderId, amount, userEmail);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail,
                            new CorrelationId(NewOrderMain.class.getSimpleName()), order);

                    var email = "Thanks! We are processing your order!";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userEmail,
                            new CorrelationId(NewOrderMain.class.getSimpleName()), email);
                }
            }
        }
    }

}
