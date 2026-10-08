package cl.rutaexpress.audit.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declara la cola q.audit.deadletter y la enlaza al exchange de mensajes muertos.
 *
 * Ruta de un mensaje muerto: notify rechaza (NACK sin requeue) un mensaje de
 * q.cmd.email / q.cmd.warehouse / q.cmd.label → RabbitMQ lo publica en cmd.dead.dlx
 * conservando su routing key original → cmd.dead.dlx lo entrega a la DLQ de su caso
 * de uso y, con estos 3 bindings, también a q.audit.deadletter. Las DLQ no se vacían:
 * esta cola recibe una copia.
 *
 * El exchange se declara con los mismos atributos que usa notify (direct, durable,
 * sin auto-delete). Si difieren, RabbitMQ rechaza la declaración con PRECONDITION_FAILED.
 */
@Configuration
public class RabbitDeadLetterConfig {

    @Value("${rutaexpress.rabbitmq.exchanges.dead-letter}")
    private String deadLetterExchange;

    @Value("${rutaexpress.rabbitmq.audit.dead-letter-queue}")
    private String auditDeadLetterQueue;

    @Value("${rutaexpress.rabbitmq.email.routing-key}")
    private String emailRoutingKey;

    @Value("${rutaexpress.rabbitmq.warehouse.routing-key}")
    private String warehouseRoutingKey;

    @Value("${rutaexpress.rabbitmq.label.routing-key}")
    private String labelRoutingKey;

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(deadLetterExchange, true, false);
    }

    @Bean
    public Queue auditDeadLetterQueue() {
        return QueueBuilder.durable(auditDeadLetterQueue).build();
    }

    @Bean
    public Declarables auditDeadLetterBindings(DirectExchange deadLetterExchange, Queue auditDeadLetterQueue) {
        Binding email = BindingBuilder.bind(auditDeadLetterQueue).to(deadLetterExchange).with(emailRoutingKey);
        Binding warehouse = BindingBuilder.bind(auditDeadLetterQueue).to(deadLetterExchange).with(warehouseRoutingKey);
        Binding label = BindingBuilder.bind(auditDeadLetterQueue).to(deadLetterExchange).with(labelRoutingKey);
        return new Declarables(email, warehouse, label);
    }
}