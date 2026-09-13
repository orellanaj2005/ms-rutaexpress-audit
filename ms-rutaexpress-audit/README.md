# ms-rutaexpress-audit

Microservicio de auditoría de RutaExpress. Consume eventos de Kafka
(`shipments.events` y `audit.timeline`) y expone el timeline de
trazabilidad de cada envío, de solo lectura, para los roles **Admin** y
**Auditor**.

## Variables de entorno (`.env`)

```
TENANT_ID=<tenantId de Azure AD, te lo pasa Javier>
API_CLIENT_ID=<apiClientId, sin el prefijo api://>
DB_HOST=localhost
DB_PORT=1521
DB_SERVICE=FREEPDB1
DB_USER=audit_user
DB_PASSWORD=changeit
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

## Pendiente / a confirmar con el equipo

- [ ] Confirmar con Jassack los nombres exactos de los campos del `payload`
      en `shipments.events` (`shipmentId`, `actor`/`userId`) — el consumidor
      ahora mismo asume esos nombres, ajustar si el envelope real usa otros.
- [ ] Confirmar quién produce el tópico `audit.timeline` y con qué forma de
      payload (no está resuelto en la guía de Javier todavía).
- [ ] `DB_USER` propio para este microservicio (no compartir esquema con
      `report`/`shipments`/`catalog` — rompe Flyway, según la nota de Jassack).
- [ ] Levantar contra Kafka real una vez que Javier tenga el tópico
      `shipments.events` creado (punto de sincronización #2 del reparto).

## Correr local

```bash
mvn spring-boot:run
```

Requiere Oracle y Kafka accesibles (usa el `docker-compose` del repo `ms-rutaexpress-db` / `infra` de Javier).

## Endpoints

- `GET /api/audit/timeline?actor=&eventType=&shipmentId=&from=&to=&page=&size=`
  — requiere rol `Admin` o `Auditor`.
