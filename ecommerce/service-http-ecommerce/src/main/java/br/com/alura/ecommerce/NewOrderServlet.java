package br.com.alura.ecommerce;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        var order = createOrder(req);

        dispatchOrder(order);

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().println("New order sent successfully");
    }

    private void dispatchOrder(Order order) throws ServletException {
        try {
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", order.getEmail(),
                    new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

            var email = "Thanks! We are processing your order!";
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(),
                    new CorrelationId(NewOrderServlet.class.getSimpleName()), email);

            System.out.println("New order sent successfully");
        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }
    }

    private Order createOrder(HttpServletRequest req) {
        var email = req.getParameter("email");
        var amount = new BigDecimal(req.getParameter("amount"));
        var orderId = UUID.randomUUID().toString();

        return new Order(orderId, amount, email);
    }
}
