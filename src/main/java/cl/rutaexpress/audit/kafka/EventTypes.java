package cl.rutaexpress.audit.kafka;

/** Valores del campo {@code type} del envelope (plan Kafka, sección 3). */
public final class EventTypes {

    public static final String SHIPMENT_CREATED = "ShipmentCreated";
    public static final String SHIPMENT_STATUS_CHANGED = "ShipmentStatusChanged";
    public static final String AUDIT_ACTION = "AuditAction";

    private EventTypes() {
    }
}
