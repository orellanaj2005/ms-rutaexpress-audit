package cl.rutaexpress.audit.controller;

import cl.rutaexpress.audit.dto.AuditEventResponse;
import cl.rutaexpress.audit.repository.AuditEventRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

/**
 * Solo lectura (ver Caso 3, sección 3): expone el timeline de auditoría
 * con filtros por usuario, fecha y tipo de evento, tal como pide la
 * pantalla /audit del frontend.
 */
@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private final AuditEventRepository repository;

    public AuditController(AuditEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/timeline")
    public Page<AuditEventResponse> timeline(
            @RequestParam(required = false) String actor,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) String shipmentId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return repository
                .search(actor, eventType, shipmentId, from, to, PageRequest.of(page, size))
                .map(AuditEventResponse::from);
    }
}
