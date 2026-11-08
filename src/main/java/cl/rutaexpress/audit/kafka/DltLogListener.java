package cl.rutaexpress.audit.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Escucha la DLT del servicio y deja en el log la causa del fallo (headers que agrega
 * DeadLetterPublishingRecoverer). Lee el valor como String para que un mensaje que no
 * se pudo deserializar no vuelva a fallar acá.
 */
@Component
public class DltLogListener {

    private static final Logger log = LoggerFactory.getLogger(DltLogListener.class);
    private static final int MAX_PAYLOAD_CHARS = 500;

    @KafkaListener(
            topics = "${rutaexpress.kafka.dlt-topic}",
            groupId = "${spring.kafka.consumer.group-id}-dlt",
            containerFactory = "dltListenerContainerFactory"
    )
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        log.error("DLT {}-{}@{} | origen={}-{}@{} | excepción={} | causa={} | key={} | valor={}",
                record.topic(), record.partition(), record.offset(),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                header(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                header(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                header(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.key(),
                truncate(record.value()));
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null) {
            return "-";
        }
        byte[] v = h.value();
        // partition y offset vienen como número binario (4 y 8 bytes); el resto son texto.
        if (KafkaHeaders.DLT_ORIGINAL_PARTITION.equals(name) && v.length == Integer.BYTES) {
            return String.valueOf(java.nio.ByteBuffer.wrap(v).getInt());
        }
        if (KafkaHeaders.DLT_ORIGINAL_OFFSET.equals(name) && v.length == Long.BYTES) {
            return String.valueOf(java.nio.ByteBuffer.wrap(v).getLong());
        }
        return new String(v, StandardCharsets.UTF_8);
    }

    private static String truncate(String value) {
        if (value == null) {
            return "null";
        }
        return value.length() <= MAX_PAYLOAD_CHARS ? value : value.substring(0, MAX_PAYLOAD_CHARS) + "...";
    }
}

