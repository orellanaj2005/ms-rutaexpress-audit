package cl.rutaexpress.audit.kafka;

import cl.rutaexpress.audit.dto.EventEnvelope;
import cl.rutaexpress.audit.exception.InvalidEventException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Valida el envelope y convierte su payload al record tipado que corresponde al {@code type}.
 * Cualquier problema se lanza como {@link InvalidEventException} (no reintentable -> DLT).
 */
@Component
public class EventValidator {

    private final Validator validator;
    private final ObjectMapper objectMapper;

    public EventValidator(Validator validator, ObjectMapper objectMapper) {
        this.validator = validator;
        this.objectMapper = objectMapper;
    }

    public void validateEnvelope(EventEnvelope envelope) {
        if (envelope == null) {
            throw new InvalidEventException("El mensaje no trae envelope (valor nulo)");
        }
        check(envelope, "envelope");
    }

    /** Convierte el payload al tipo pedido y valida sus anotaciones (@NotBlank, etc.). */
    public <T> T payload(EventEnvelope envelope, Class<T> type) {
        T value;
        try {
            value = objectMapper.treeToValue(envelope.payload(), type);
        } catch (JsonProcessingException | IllegalArgumentException ex) {
            throw new InvalidEventException("Payload inválido para type=" + envelope.type()
                    + ": " + ex.getMessage(), ex);
        }
        if (value == null) {
            throw new InvalidEventException("Payload vacío para type=" + envelope.type());
        }
        check(value, "payload " + envelope.type());
        return value;
    }

    private void check(Object target, String what) {
        Set<ConstraintViolation<Object>> violations = validator.validate(target);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            throw new InvalidEventException("Validación fallida en " + what + ": " + detail);
        }
    }
}
