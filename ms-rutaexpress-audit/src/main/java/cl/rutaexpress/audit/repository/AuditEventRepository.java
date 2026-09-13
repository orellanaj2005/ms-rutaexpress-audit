package cl.rutaexpress.audit.repository;

import cl.rutaexpress.audit.domain.AuditEvent;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;

public interface AuditEventRepository extends JpaRepository<AuditEvent, Long> {

    boolean existsByEventId(String eventId);

    Optional<AuditEvent> findByEventId(String eventId);

    // Filtros de la pantalla de Auditoría: usuario, fechas, tipo de evento (ver Caso 3, sección 6)
    @Query("""
            SELECT a FROM AuditEvent a
            WHERE (:actor IS NULL OR a.actor = :actor)
              AND (:eventType IS NULL OR a.eventType = :eventType)
              AND (:shipmentId IS NULL OR a.shipmentId = :shipmentId)
              AND (:from IS NULL OR a.occurredAt >= :from)
              AND (:to IS NULL OR a.occurredAt <= :to)
            ORDER BY a.occurredAt DESC
            """)
    Page<AuditEvent> search(
            @Param("actor") String actor,
            @Param("eventType") String eventType,
            @Param("shipmentId") String shipmentId,
            @Param("from") Instant from,
            @Param("to") Instant to,
            Pageable pageable
    );
}
