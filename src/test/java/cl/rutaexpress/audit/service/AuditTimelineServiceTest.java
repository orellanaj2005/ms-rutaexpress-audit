package cl.rutaexpress.audit.service;

import cl.rutaexpress.audit.domain.AuditEvent;
import cl.rutaexpress.audit.exception.InvalidEventException;
import cl.rutaexpress.audit.repository.AuditEventRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static cl.rutaexpress.audit.kafka.EventTestSupport.TS;
import static cl.rutaexpress.audit.kafka.EventTestSupport.envelope;
import static cl.rutaexpress.audit.kafka.EventTestSupport.validator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuditTimelineServiceTest {

    private final AuditEventRepository repository = mock(AuditEventRepository.class);
    private final AuditTimelineService service = new AuditTimelineService(repository, validator());

    private AuditEvent saved() {
        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(repository).saveAndFlush(captor.capture());
        return captor.getValue();
    }

    @Test
    void shipmentCreatedSeGuardaConShipmentIdYActor() {
        var env = envelope("evt-1", "ShipmentCreated",
                "{\"shipmentId\":\"SHP-9\",\"serviceType\":\"EXPRESS\",\"userId\":\"u-77\"}");

        var result = service.record(env, "shipments.events");

        assertThat(result).isEqualTo(AuditTimelineService.Result.SAVED);
        AuditEvent e = saved();
        assertThat(e.getEventId()).isEqualTo("evt-1");
        assertThat(e.getEventType()).isEqualTo("ShipmentCreated");
        assertThat(e.getSourceTopic()).isEqualTo("shipments.events");
        assertThat(e.getShipmentId()).isEqualTo("SHP-9");
        assertThat(e.getActor()).isEqualTo("u-77");
        assertThat(e.getOccurredAt()).isEqualTo(TS);
        assertThat(e.getTraceId()).isEqualTo("trace-1");
        assertThat(e.getCorrelationId()).isEqualTo("corr-1");
    }

    @Test
    void shipmentStatusChangedSeGuardaConActor() {
        var env = envelope("evt-2", "ShipmentStatusChanged",
                "{\"shipmentId\":\"SHP-9\",\"status\":\"EN_RUTA\",\"actor\":\"chofer-1\"}");

        service.record(env, "shipments.events");

        AuditEvent e = saved();
        assertThat(e.getShipmentId()).isEqualTo("SHP-9");
        assertThat(e.getActor()).isEqualTo("chofer-1");
    }

    @Test
    void auditActionSeGuardaDesdeAuditTimeline() {
        var env = envelope("evt-3", "AuditAction",
                "{\"action\":\"RESET_OFFSETS\",\"actor\":\"admin@rutaexpress.cl\"}");

        service.record(env, "audit.timeline");

        AuditEvent e = saved();
        assertThat(e.getSourceTopic()).isEqualTo("audit.timeline");
        assertThat(e.getActor()).isEqualTo("admin@rutaexpress.cl");
        assertThat(e.getShipmentId()).isEqualTo("SHP-1"); // cae al aggregateId del envelope
    }

    @Test
    void tipoDesconocidoIgualSeGuarda() {
        var env = envelope("evt-4", "AlgoNuevo", "{\"cualquier\":\"cosa\"}");

        var result = service.record(env, "shipments.events");

        assertThat(result).isEqualTo(AuditTimelineService.Result.SAVED);
        assertThat(saved().getEventType()).isEqualTo("AlgoNuevo");
    }

    @Test
    void eventoDuplicadoNoSeVuelveAGuardar() {
        when(repository.existsByEventId("evt-1")).thenReturn(true);
        var env = envelope("evt-1", "ShipmentCreated", "{\"shipmentId\":\"SHP-9\"}");

        var result = service.record(env, "shipments.events");

        assertThat(result).isEqualTo(AuditTimelineService.Result.DUPLICATE);
        verify(repository, never()).saveAndFlush(any());
    }

    @Test
    void payloadSinShipmentIdEsInvalido() {
        var env = envelope("evt-5", "ShipmentCreated", "{\"serviceType\":\"EXPRESS\"}");

        assertThatThrownBy(() -> service.record(env, "shipments.events"))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("shipmentId");
        verify(repository, never()).saveAndFlush(any());
    }

    @Test
    void statusChangedConNewStatusEnVezDeStatusEsInvalido() {
        var env = envelope("evt-6", "ShipmentStatusChanged",
                "{\"shipmentId\":\"SHP-9\",\"newStatus\":\"EN_RUTA\"}");

        assertThatThrownBy(() -> service.record(env, "shipments.events"))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("status");
    }

    @Test
    void eventoSinEventIdEsInvalido() {
        var env = envelope(null, "ShipmentCreated", "{\"shipmentId\":\"SHP-9\"}");

        assertThatThrownBy(() -> service.record(env, "shipments.events"))
                .isInstanceOf(InvalidEventException.class);
        verify(repository, never()).saveAndFlush(any());
    }
}
