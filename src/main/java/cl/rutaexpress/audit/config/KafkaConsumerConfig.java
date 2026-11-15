package cl.rutaexpress.audit.config;

import cl.rutaexpress.audit.dto.EventEnvelope;
import cl.rutaexpress.audit.exception.InvalidEventException;
import cl.rutaexpress.audit.kafka.LoggingRetryListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.BackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumo de Kafka con manejo de errores (plan Kafka, tarea 4.8):
 *  - ErrorHandlingDeserializer: un mensaje ilegible no tumba el consumer, llega como error al handler.
 *  - Errores no reintentables (evento inválido / no deserializable) van directo a la DLT.
 *  - Errores transitorios (BD caída, etc.): 3 reintentos con backoff exponencial 1 s, 2 s, 4 s y luego DLT.
 *  - 3 hilos de consumo (uno por partición de shipments.events).
 *  - ACK manual inmediato.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${rutaexpress.kafka.dlt-topic}")
    private String dltTopic;

    @Value("${rutaexpress.kafka.concurrency:3}")
    private int concurrency;

    @Bean
    public ConsumerFactory<String, EventEnvelope> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "cl.rutaexpress.audit.dto");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventEnvelope.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Template solo para publicar en la DLT. Un mensaje que falló al deserializar viaja como byte[]
     * (los bytes originales), uno ya deserializado como EventEnvelope -> JSON.
     */
    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        JsonSerializer<Object> json = new JsonSerializer<>();
        json.setAddTypeInfo(false);
        Map<Class<?>, org.apache.kafka.common.serialization.Serializer<?>> delegates = new HashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(EventEnvelope.class, json);
        delegates.put(String.class, new StringSerializer());

        ProducerFactory<String, Object> pf = new DefaultKafkaProducerFactory<>(
                props, new StringSerializer(), new DelegatingByTypeSerializer(delegates));
        return new KafkaTemplate<>(pf);
    }

    /** Backoff exponencial: 1 s, 2 s, 4 s y se acaban los reintentos. */
    public static BackOff retryBackOff() {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10000L);
        return backOff;
    }

    /** Todo lo que falla en este servicio va a la misma DLT (audit.DLT / report.DLT / ...). */
    public static DefaultErrorHandler buildErrorHandler(KafkaOperations<?, ?> template, String dltTopic) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template, (record, ex) -> new TopicPartition(dltTopic, -1)); // -1: que Kafka elija la partición

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, retryBackOff());
        // DeserializationException, MessageConversionException, ClassCastException... ya vienen como
        // no reintentables por defecto. Se suma el nuestro.
        handler.addNotRetryableExceptions(InvalidEventException.class);
        handler.setRetryListeners(new LoggingRetryListener());
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> kafkaListenerContainerFactory(
            ConsumerFactory<String, EventEnvelope> consumerFactory,
            KafkaTemplate<String, Object> dltKafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(buildErrorHandler(dltKafkaTemplate, dltTopic));
        return factory;
    }

    /** Factory aparte para leer la DLT como texto plano (no JSON) y solo dejarla en el log. */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dltListenerContainerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-dlt");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        return factory;
    }
}
