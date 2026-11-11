# Validación del productor Kafka — EV5/EV6

## Resultado ejecutado (2026-11-08, America/Santiago)

Comando desde `barriodigitalrequests/`:

```powershell
.\mvnw.cmd -B -ntp '-Dspring.config.import=' '-Dapi.version=1.44' -P kafka-postgres-it,report-contract-it verify
```

**BUILD SUCCESS**. Compilación Java 21 y empaquetado Spring Boot 3.3.6 completados. Resultado: **63 pruebas, 0 fallos, 0 errores, 0 omitidas** (50 Surefire + 13 Failsafe).

| Evidencia | Resultado y alcance |
| --- | --- |
| Suite previa: 44 pruebas | Pasó. Endpoints, lectura/permisos, filtros, transiciones, DTO y publicación RabbitMQ con dobles de prueba. |
| `KafkaEventJsonTest`: 3 | Campos exactos, fecha UTC, validación y backoff acotado. |
| `CatalogNameResolverTest`: 2 | HTTP local de prueba: contrato Catalog, 404, ID no numérico y respuesta de otro procedimiento. No consulta Catalog/Supabase reales. |
| `KafkaDisabledTest`: 1 | Creación/cambio sobre esquema anterior H2 sin tablas Kafka ni worker; conserva createdBy. |
| `KafkaOutboxPostgresIT`: 12 | PostgreSQL real **aislado** `postgres:17.6-alpine`, máximo 256 MiB, puerto aleatorio. Migración, atomicidad y concurrencia reales. Kafka y RabbitTemplate simulados. |
| `ReportContractIT`: 1 | Compila las tres fuentes DTO reales de ms-report en target de Requests; los JSON de creación y estados válidos pasan deserialización estricta Jackson 2 y Jakarta Validation contra esas clases. No ejecuta EventCodec Jackson 3 ni el consumidor de Report. |

Las 12 pruebas PostgreSQL cubren: creación y cadena válida de cinco versiones; rechazo de transición; rollback externo sin evento huérfano; rollback por fallo de inserción outbox; Catalog caído y recuperación; Kafka caído y reenvío del mismo JSON sin adelantar versión; registros antiguos retenidos; detección de hueco al desactivar captura; dos actualizaciones concurrentes con un único ganador; bloqueo entre workers; reenvío tras ACK simulado y rollback del marcador; y IDs admitidos por Requests pero demasiado largos para Report.

Dependencias efectivamente resueltas por el BOM, sin sobrescribir versiones: `spring-kafka 3.2.5`, `kafka-clients 3.7.1`, `testcontainers 1.19.8` y driver PostgreSQL `42.7.4`. Se conserva el broker existente 3.9.1; no se instala Kafka ni se crean tópicos con estas pruebas. Uso de CompletableFuture acorde a [KafkaTemplate 3.2.5](https://docs.spring.io/spring-kafka/docs/3.2.5/api/org/springframework/kafka/core/KafkaTemplate.html).

Hubo un fallo inicial de acceso a Docker 29 con la API predeterminada de Testcontainers; se resolvió para la ejecución con `-Dapi.version=1.44`. También se corrigieron dos problemas del test (callback de inicialización de Spring y reconfiguración de un mock que lanzaba excepción). El resultado anterior corresponde a la ejecución completa posterior a esas correcciones.

Los logs y reportes locales están en `barriodigitalrequests/target/kafka-full-validation.log`, `target/surefire-reports/` y `target/failsafe-reports/`; target no se versiona. Las pruebas usan `spring.config.import=` y datasources aislados. No se leyó ningún `.env` real ni se ejecutaron escrituras sobre Supabase.

## Qué todavía no está verificado

- No se aplicó la migración en Supabase. Solo se ejecutó en el PostgreSQL temporal de las pruebas.
- No se arrancó Requests contra la base compartida con Kafka habilitado.
- No se publicó ningún mensaje real en `bd.requests.events.v1` ni `bd.requests.analytics.v1` durante esta implementación. Los mocks de Kafka no prueban conectividad, ACK real ni el circuito completo.
- No se probó RabbitMQ/Notify contra servicios reales; se verificó regresión automatizada y ausencia de cambios en sus contratos/código existente.
- No se ejecutó un backfill histórico ni se resolvieron equivalencias de procedureTypeId antiguos no numéricos.

El próximo paso operativo es revisar/aplicar la migración y activar explícitamente el productor según [README-Kafka.md](README-Kafka.md). La bandera sigue en `false` por defecto.

## Verificación local del circuito real (procedimiento manual)

Ejecutar únicamente después de la revisión de migración y configuración. Esta prueba **crea una solicitud y dispara las notificaciones existentes**, por lo que debe hacerse con datos y entorno de prueba acordados. Si Report está conectado a Supabase compartido, también persistirá esos eventos; no es una prueba de solo lectura. No reutilizar IDs/eventIds de ejemplos ni publicar JSON arbitrarios en tópicos compartidos.

1. Verificar Kafka/ZooKeeper/Kafbat existentes, Catalog, Requests, RabbitMQ/Notify y Report con sus configuraciones habituales. No recrear tópicos ni redes. Activar Requests con los comandos de README-Kafka. Usar otra terminal para los pasos siguientes.
2. Elegir un procedimiento real de Catalog y comprobar su nombre:

   ```powershell
   $procedureId = Read-Host 'ID numérico real del procedimiento de prueba'
   $procedure = Invoke-RestMethod "http://localhost:8082/api/catalog/procedures/$procedureId"
   $procedure | Select-Object id,name
   ```

3. Crear una solicitud controlada. Los encabezados de este ejemplo corresponden al acceso local ya existente; en el despliegue se conservan los controles BFF/Entra actuales:

   ```powershell
   $testEmail = Read-Host 'Correo autorizado para la prueba y sus notificaciones'
   $headers = @{ 'X-User-Email'=$testEmail }
   $body = @{ procedureTypeId=[string]$procedure.id; description='Validación acordada del flujo Kafka EV5/EV6' } | ConvertTo-Json
   $created = Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/api/requests' -Headers $headers -ContentType 'application/json' -Body $body
   $requestId = $created.id
   $created
   ```

4. Consultar outbox, sustituyendo `:request_id` por ese ID en el cliente SQL (es un parámetro ilustrativo):

   ```sql
   SELECT event_id, request_id, request_version, event_type, previous_status,
          status, published_at, attempts, available_at, last_error
   FROM bd_requests_kafka.outbox WHERE request_id=:request_id ORDER BY request_version;
   SELECT request_id, tracking_status, last_version, last_status, procedure_id, procedure_name, blocked_reason
   FROM bd_requests_kafka.stream WHERE request_id=:request_id;
   ```

   Esperar creación versión 1, previousStatus null, INGRESADO y, tras ACK, published_at no nulo. Mientras Catalog/Kafka no están disponibles, published_at queda nulo y last_error/attempts explican la espera.

5. En Kafbat UI, abrir `bd.requests.events.v1`, filtrar la clave decimal de requestId y revisar `REQUEST_CREATED`, nombre real y UTC. Revisar partición/offset sin cambiar configuración. La configuración existente debe seguir con tres particiones, replicación tres e ISR saludables.
6. Ejecutar un cambio válido y luego, si la prueba acordada requiere recorrer todo el flujo, los restantes:

   ```powershell
   foreach ($nextStatus in @('ADMITIDO','EN_GESTION','EN_TERRENO','RESUELTO')) {
       $change = @{ status=$nextStatus } | ConvertTo-Json
       Invoke-RestMethod -Method Put -Uri "http://localhost:8081/api/requests/$requestId/status" -ContentType 'application/json' -Body $change
   }
   ```

   Deben existir versiones 1–5 con estados anteriores correctos y la misma clave/partición. No adelantar un evento pendiente. Pueden aparecer duplicados con el mismo eventId/JSON después de fallos ambiguos, sin duplicar efectos en Report. Un cambio inválido devuelve el error HTTP existente sin agregar outbox.

7. Confirmar aplicación en Report mediante lecturas:

   ```sql
   SELECT event_id, request_id, request_version, event_type, source_partition, source_offset
   FROM bd_report.processed_event WHERE request_id=:request_id ORDER BY request_version;
   SELECT request_id, request_version, procedure_id, procedure_name, status,
          created_at, resolved_at, resolution_seconds
   FROM bd_report.request_projection WHERE request_id=:request_id;
   SELECT source_event_id, request_id, request_version, published_at, attempts, last_error
   FROM bd_report.analytics_outbox WHERE request_id=:request_id ORDER BY request_version;
   SELECT event_id, request_id, failure_type, failure_reason
   FROM bd_report.failed_event WHERE request_id=CAST(:request_id AS text);
   ```

   ```powershell
   Invoke-RestMethod 'http://localhost:8083/api/report/kpis?range=last24h'
   Invoke-RestMethod 'http://localhost:8083/api/report/top-procedures?range=last7d'
   ```

   Consultar Kafbat → `bd.requests.analytics.v1` para los eventos derivados publicados por Report. El ACK del productor Requests no basta: verificar processed_event, proyección y analytics_outbox confirma los pasos posteriores. Comparar por requestId/eventId, sin asumir incrementos globales exactos si hay otro tráfico.

8. Confirmar en RabbitMQ/Notify las notificaciones correspondientes con el procedimiento EP4 existente. Mantener independientes sus colas, bindings y DLQ.

## Prueba de recuperación sin detener brokers

En un entorno de prueba, dejar captura habilitada y arrancar Requests con `KAFKA_BOOTSTRAP_SERVERS=localhost:1` temporalmente. Crear una solicitud controlada: la escritura y RabbitMQ deben seguir, mientras outbox registra intentos y no publicado. Restaurar los tres bootstrap servers correctos y reiniciar Requests; comprobar que el mismo eventId/JSON llega y se marca publicado. No usar esta prueba en una instancia que atienda tráfico compartido sin coordinación.

Para probar Catalog, usar temporalmente una URL local inaccesible, generar una solicitud y restaurar la URL real. El evento queda sin payload hasta resolver un nombre válido. No marcar manualmente published_at para simular éxito. La condición de crash después de ACK y antes de commit está cubierta con PostgreSQL real y ACK simulado; una prueba de caída real requiere un entorno aislado adicional y no se ejecutó aquí.

## Consultas operativas de solo lectura

```sql
SELECT count(*) AS pending, min(occurred_at) AS oldest_pending,
       max(attempts) AS max_attempts
FROM bd_requests_kafka.outbox WHERE published_at IS NULL;
SELECT last_error, count(*) FROM bd_requests_kafka.outbox
WHERE published_at IS NULL GROUP BY last_error;
SELECT request_id, blocked_reason FROM bd_requests_kafka.stream
WHERE tracking_status='BLOCKED_HISTORY';
SELECT request_id, event_id, previous_status, status, occurred_at, reason
FROM bd_requests_kafka.history_hold ORDER BY sequence_id;
```

No borrar filas para resolver errores. Para pausar solo publicación usar worker-enabled=false con captura activa; para historial incompleto seguir la estrategia revisada por solicitud del README.

## Inventario de cambios

- Modificados: `README.md`, `barriodigitalrequests/pom.xml`, `.env.example`, `application.properties`, `RequestService.java` y `RequestRepository.java`.
- Nuevos componentes en `kafka/`: `KafkaEventCapture`, `DisabledKafkaEventCapture`, `TransactionalKafkaEventCapture`, `RequestKafkaEventV1`, `KafkaPublisherProperties`, `KafkaPublisherConfig`, `CatalogNameResolver`, `KafkaEventJson`, `KafkaOutboxStore`, `KafkaOutboxWorker` y `KafkaOutboxScheduler`.
- Migración manual: `sql/kafka/V001__requests_kafka_outbox.sql`.
- Pruebas: los cinco archivos Kafka indicados arriba y `src/test/resources/kafka/requests-existing.sql`, fixture exclusivo del contenedor aislado.
- Documentación nueva: `README-Kafka.md` y este documento.

No se modifican entidades/DTO públicos, controladores ni archivos RabbitMQ existentes. Todos los archivos creados/modificados están dentro de `ms-barriodigital-requests`; no se hicieron commits ni pushes.
