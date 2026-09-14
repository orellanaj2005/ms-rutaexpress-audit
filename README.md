# ms-rutaexpress-audit

Microservicio de auditoría de RutaExpress. Consume eventos de Kafka
(`shipments.events` y `audit.timeline`) y expone el timeline de
trazabilidad de cada envío, de solo lectura, para los roles **Admin** y
**Auditor**.

## Variables de entorno (`.env`)

```
TENANT_ID=<tenantId de Azure AD>
API_CLIENT_ID=<apiClientId>
DB_HOST=localhost
DB_PORT=1521

```



## Correr local

```bash
mvn spring-boot:run
```

Requiere Oracle y Kafka accesibles (usa el `docker-compose` del repo `ms-rutaexpress-db` /).

## Endpoints

- `GET /api/audit/timeline?actor=&eventType=&shipmentId=&from=&to=&page=&size=`
  — requiere rol `Admin` o `Auditor`.
