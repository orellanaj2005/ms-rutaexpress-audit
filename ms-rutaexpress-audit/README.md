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



## Correr local

```bash
mvn spring-boot:run
```

Requiere Oracle y Kafka accesibles (usa el `docker-compose` del repo `ms-rutaexpress-db` / `infra` de Javier).

## Endpoints

- `GET /api/audit/timeline?actor=&eventType=&shipmentId=&from=&to=&page=&size=`
  — requiere rol `Admin` o `Auditor`.
