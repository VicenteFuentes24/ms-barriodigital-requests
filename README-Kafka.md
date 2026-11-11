# Productor Kafka de Requests — EV5/EV6

Java 21 y Spring Boot 3.3.6 se conservan. Spring Kafka y Testcontainers (solo pruebas) usan las versiones del BOM de Boot. No se modifican Report, Catalog, RabbitMQ, Notify, BFF, React ni la infraestructura. Requests solo produce en `bd.requests.events.v1`; no consume ni produce analítica. Ese segundo paso corresponde a ms-report.

## Flujo y consistencia

1. `RequestService` mantiene las validaciones, permisos, respuestas y eventos RabbitMQ existentes.
2. Con `KAFKA_PUBLISHER_ENABLED=true`, la creación/cambio válido guarda la solicitud y su outbox en **la misma transacción PostgreSQL**. `JdbcTemplate` participa en la transacción de `JpaTransactionManager` usando el mismo datasource. Si la persistencia del outbox falla, se revierte también la solicitud; no se confirma una escritura sin su evento.
3. El worker resuelve el nombre real en Catalog y guarda el JSON definitivo en una transacción independiente. Solo después de ese commit puede enviarlo. Una caída entre ambas fases deja un evento recuperable.
4. Otra transacción selecciona el primer evento pendiente de cada solicitud, envía su JSON con `KafkaTemplate`, espera el ACK del broker y marca `published_at`. El HTTP de Requests no espera a Kafka ni a Catalog; únicamente el worker realiza esas llamadas.
5. Si Kafka confirma y el proceso cae antes del commit de `published_at`, se reenvía el **mismo eventId y JSON**. Es entrega al menos una vez; Report deduplica. No existe una transacción exactamente una vez entre Kafka y PostgreSQL.

RabbitMQ sigue usando su `ApplicationEventPublisher` y `@TransactionalEventListener` originales, con el mismo DTO, factory, publisher y configuración. Kafka no reutiliza sus mensajes ni modifica sus garantías actuales. Un Kafka inaccesible no impide guardar solicitudes ni ejecutar la publicación RabbitMQ existente.

## Versionado, orden y modelo de datos

La migración `sql/kafka/V001__requests_kafka_outbox.sql` crea exclusivamente el esquema `bd_requests_kafka` con:

| Tabla | Función |
| --- | --- |
| `schema_version` | Comprobar la migración antes de habilitar captura. |
| `stream` | Versión, estado, instante y nombre confirmado por solicitud. |
| `outbox` | Evento durable, JSON congelado, intentos, próximo intento y ACK confirmado. |
| `history_hold` | Cambios observados de solicitudes sin historial verificable. No son eventos listos para Kafka. |

El versionado se guarda en una tabla auxiliar, en vez de agregar una propiedad JPA a `RequestEntity`. Así, con la bandera desactivada, las consultas y escrituras originales **no requieren columnas ni tablas nuevas**. No cambia la representación pública de las solicitudes.

Una solicitud nueva empieza en versión 1. Para cambios se bloquea su fila `requests` mediante `PESSIMISTIC_WRITE`; se valida la transición después de adquirir el bloqueo y se asigna exactamente la siguiente versión. La restricción `UNIQUE(request_id,request_version)` refuerza la integridad. Las transiciones rechazadas no asignan versión ni evento. `updatedAt` no se usa como versión.

Los workers usan `FOR UPDATE OF o SKIP LOCKED` y excluyen filas con cualquier versión anterior sin publicar, incluso si está en backoff o bloqueada por otro worker. Pueden trabajar distintas instancias, pero una versión posterior no adelanta a una anterior pendiente. La clave Kafka es siempre `Long.toString(requestId)`, sin prefijos ni ceros añadidos. El bloqueo de envío se limita a la fila del outbox, no a la solicitud. Los duplicados tras un fallo ambiguo se resuelven en Report por eventId.

`occurredAt` se escribe como UTC y precisión de microsegundos; los cambios no retroceden respecto al último instante capturado. El nombre y JSON quedan congelados antes del primer envío para conservar el hash esperado por la deduplicación de Report.

## Contrato con Report

`kafka/RequestKafkaEventV1` contiene exactamente los diez campos de `ms-report/dto/RequestEventV1`: eventId UUID, eventType, schemaVersion=1, requestId positivo, requestVersion positivo, procedureId (1–100 caracteres), procedureName (1–250), previousStatus, status y occurredAt ISO-8601 UTC.

- `REQUEST_CREATED`: versión 1, previousStatus=null, status=INGRESADO.
- `REQUEST_STATUS_CHANGED`: versión anterior + 1 y estado anterior real distinto del nuevo.
- Estados JSON: `INGRESADO`, `ADMITIDO`, `EN_GESTION`, `EN_TERRENO`, `RESUELTO`, `RECHAZADO`.
- No se añaden datos personales, campos RabbitMQ ni encabezados de tipo Java. Los valores se serializan como String/JSON.

El perfil `report-contract-it` lee las fuentes DTO reales del repositorio vecino, compila una copia en `barriodigitalrequests/target/` y valida los JSON generados contra esas clases. No escribe ni compila dentro de ms-report. No equivale a ejecutar su consumidor, su codec Jackson 3 ni su persistencia.

## Nombre real del procedimiento

Catalog ofrece `GET /api/catalog/procedures/{id}` con `id` Long y respuesta con `id` y `name`. El worker lo consulta por HTTP con timeout acotado, comprueba ambos campos y guarda el nombre por solicitud. No consulta directamente la base de Catalog ni agrega un cliente de discovery.

Si Catalog está caído, devuelve 404, el nombre es inválido o `procedureTypeId` no es un identificador numérico positivo compatible, la solicitud y el evento quedan persistidos y el worker reintenta. Nunca envía un nombre inventado ni un JSON incompleto. Requests históricamente acepta IDs de hasta 120 caracteres; Report solo 100: se preserva el API de Requests y esos casos quedan pendientes con `PROCEDURE_ID_EXCEEDS_REPORT_LIMIT`, sin truncamiento.

El nombre representa el primer valor confirmado por Catalog, **no necesariamente su valor histórico en occurredAt**. Se conserva para las siguientes versiones. Para IDs antiguos tipo slug se necesita una política explícita y auditada de correspondencia con Catalog; no se inventa esa equivalencia. No cambiar un payload que ya pudo enviarse. Corregir el dato autoritativo/mapeo debe ser una operación revisada; estos casos siguen visibles y recuperables en outbox.

## Migración manual y activación segura

La aplicación no ejecuta Flyway ni este SQL automáticamente. El script está fuera de `src/main/resources`. No se adopta una línea base Flyway sobre tablas preexistentes.

1. Mantener `KAFKA_PUBLISHER_ENABLED=false`. Revisar y respaldar la base con el procedimiento habitual del equipo. Confirmar que Requests usa `public.requests` y que `id` es bigint. Si el esquema real difiere, revisar el SQL antes de aplicarlo; no renombrar tablas existentes.
2. Revisar `sql/kafka/V001__requests_kafka_outbox.sql` y ejecutarlo **una sola vez** mediante una conexión administrativa autorizada a la base correcta. Incluye BEGIN/COMMIT y `lock_timeout=5s`. Crea tablas e índices nuevos y claves foráneas, sin UPDATE/DELETE sobre datos existentes. Reaplicarlo falla explícitamente; no es un mecanismo para reconstruir tablas.
3. Verificar con consultas de lectura:

   ```sql
   SELECT current_database(), current_user;
   SELECT column_name, data_type FROM information_schema.columns
   WHERE table_schema='public' AND table_name='requests' AND column_name='id';
   SELECT * FROM bd_requests_kafka.schema_version;
   SELECT table_name FROM information_schema.tables WHERE table_schema='bd_requests_kafka';
   SELECT count(*) FROM bd_requests_kafka.outbox;
   ```

4. El usuario JDBC requiere USAGE sobre el esquema, SELECT sobre schema_version y SELECT/INSERT/UPDATE sobre las tablas operativas; también USAGE/SELECT sobre la secuencia de history_hold. Asignar permisos al rol real mediante la administración habitual; el script no inventa roles ni otorga acceso público. Mantener estas tablas internas sin exponerlas a clientes Supabase anónimos.
5. Configurar `JPA_DDL_AUTO=validate`, comprobar Catalog y las direcciones Kafka. Para PostgreSQL usar la conexión transaccional JDBC ya operativa de Requests, con SSL según corresponda. No hay credenciales en este cambio.
6. Activar captura en **todas** las instancias de Requests de forma coordinada. Evitar tráfico de escritura durante el cambio de versión/configuración. La aplicación habilitada verifica la versión del esquema y falla al arrancar si la migración falta.
7. Verificar primero una solicitud nueva controlada siguiendo [VALIDACION-Kafka.md](VALIDACION-Kafka.md). No iniciar un backfill masivo.

El SQL fue diseñado para revisión y aplicación manual en Supabase; las ejecuciones automatizadas usan exclusivamente un PostgreSQL aislado de Testcontainers.

## Solicitudes existentes e historial incompleto

No se envía un cambio como versión 1, ni se inventa un REQUEST_CREATED para un registro ya avanzado. Al observar un cambio de una solicitud sin stream, se crea `BLOCKED_HISTORY` y se guarda el cambio real en `history_hold`. El dominio y RabbitMQ continúan; esa solicitud todavía no aparece correctamente en Report.

La sincronización inicial es una operación posterior y revisada por solicitud:

1. Recuperar de fuentes autoritativas la creación (createdAt real, procedimiento) y todos los cambios con sus estados y fechas, incluyendo los retenidos. Una fila con estado actual no prueba por sí sola todo su historial.
2. Comparar con `bd_report.processed_event` y `request_projection`: un requestId ya ingerido no puede reiniciarse ni reutilizar versiones con eventIds diferentes.
3. Preparar un script específico, revisable y probado en una base aislada. Con captura activa y worker pausado, bloquear primero `public.requests` y después su stream dentro de una transacción. Construir la cadena completa: creación INGRESADO versión 1, cambios consecutivos, estados anteriores correctos y fechas no decrecientes. Conservar eventIds/JSON ya publicados y eventIds de cambios retenidos. No modificar filas publicadas.
4. Insertar únicamente los eventos faltantes en outbox, actualizar stream al último estado/versión/fecha verificados y retirar el bloqueo solo si coincide con el registro actual. Mantener history_hold como evidencia; su copia al outbox debe ser idempotente mediante eventId. Revisar antes de ejecutar y reanudar el worker después del commit.
5. Si no existe historial confiable, mantener el bloqueo y limitar la reportería a solicitudes nuevas hasta definir una política de dominio explícita. No reconstruir transiciones ni tiempos de resolución ficticios.

No se incluye ni ejecuta un backfill automático. La misma protección detecta discrepancias de estado si hubo cambios con captura desactivada, pero no puede descubrir por sí sola todos los cambios omitidos (por ejemplo, una solicitud terminal que ya no cambia). Antes de reactivar después de un intervalo sin captura, reconciliar el historial. **Para pausar envíos usar `KAFKA_OUTBOX_WORKER_ENABLED=false`, conservando captura activa**, no desactivar el productor en un sistema ya integrado.

## Configuración

Además de las variables DB/RabbitMQ existentes, `.env.example` incorpora:

| Variable | Valor predeterminado | Uso |
| --- | --- | --- |
| `KAFKA_PUBLISHER_ENABLED` | `false` | Captura durable y componentes Kafka. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092,localhost:29092,localhost:39092` | Listeners Windows. |
| `KAFKA_REQUESTS_TOPIC` | `bd.requests.events.v1` | Validado contra el único destino autorizado. Cambiar destino requiere revisar contrato y restricción SQL. |
| `CATALOG_SERVICE_URL` | `http://localhost:8082` | Base URL de Catalog, sin credenciales. |
| `KAFKA_CATALOG_TIMEOUT_MS` | `2000` | Timeout HTTP y conexión. |
| `KAFKA_OUTBOX_WORKER_ENABLED` | `true` | Permite pausar solo entrega. |
| `KAFKA_OUTBOX_POLL_MS` | `1000` | Pausa entre lotes. |
| `KAFKA_OUTBOX_BATCH_SIZE` | `10` | Máximo de iteraciones por lote. |
| `KAFKA_OUTBOX_RETRY_INITIAL_MS` | `2000` | Backoff inicial. |
| `KAFKA_OUTBOX_RETRY_MAX_MS` | `60000` | Tope del backoff exponencial. |
| `KAFKA_SEND_TIMEOUT_MS` | `20000` | Espera del worker por ACK. |
| `KAFKA_DELIVERY_TIMEOUT_MS` | `15000` | Límite de entrega/reintentos internos del cliente Kafka. |
| `KAFKA_REQUEST_TIMEOUT_MS` | `10000` | Timeout de petición Kafka. |
| `KAFKA_MAX_BLOCK_MS` | `10000` | Espera máxima por metadata/buffer al invocar send. |
| `KAFKA_PRODUCER_BACKOFF_MS` | `500` | Backoff interno del cliente. |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Protocolo de la infraestructura local. |

Se configuran `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, StringSerializer y `spring.kafka.admin.auto-create=false`. No hay NewTopic ni operaciones administrativas. Los reintentos internos están acotados por delivery.timeout.ms; el outbox reintenta indefinidamente con backoff acotado para no perder eventos. No cambia particiones, replicación, retención ni min.insync.replicas.

Conservar `delivery.timeout.ms >= request.timeout.ms + linger.ms` y `send-timeout >= delivery.timeout`. No aumentar max.block + send-timeout hasta superar el timeout transaccional del worker (90 s). Un fallo permanente retiene el evento y bloquea las siguientes versiones **de esa solicitud**; no se descarta ni desvía a otro tópico.

En Docker/AWS configurar bootstrap servers con los listeners internos anunciados y resolubles desde el contenedor, y Catalog con su DNS interno. `localhost` dentro de un contenedor no identifica los brokers de otro contenedor. Se reutiliza la infraestructura existente; aquí no se crean redes, brokers, tópicos ni configuración AWS. SSL/SASL, si el despliegue lo requiere, se configura mediante propiedades Spring y un gestor de secretos fuera del repositorio.

## Ejecución PowerShell

Desde el workspace:

```powershell
Set-Location 'G:\Desarrollo EP3-EP4\ms-barriodigital-requests\barriodigitalrequests'
# Pruebas sin cargar .env real ni usar servicios compartidos:
.\mvnw.cmd -B -ntp '-Dspring.config.import=' test
# PostgreSQL desechable (Docker Desktop) y DTO real del repositorio vecino:
.\mvnw.cmd -B -ntp '-Dspring.config.import=' '-Dapi.version=1.44' -P kafka-postgres-it,report-contract-it verify
```

El argumento api.version permite usar Testcontainers administrado por Boot 3.3.6 con Docker 29 sin cambiar dependencias. Consultar [Docker Engine API](https://docs.docker.com/reference/api/engine/) y el [incidente de Testcontainers](https://github.com/testcontainers/testcontainers-java/issues/11210). Docker debe estar en PATH; las pruebas solo eliminan sus propios contenedores temporales.

Después de aplicar/verificar la migración y con variables DB/RabbitMQ existentes configuradas localmente:

```powershell
$env:JPA_DDL_AUTO='validate'
$env:KAFKA_PUBLISHER_ENABLED='true'
$env:KAFKA_OUTBOX_WORKER_ENABLED='true'
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:19092,localhost:29092,localhost:39092'
$env:KAFKA_REQUESTS_TOPIC='bd.requests.events.v1'
$env:CATALOG_SERVICE_URL='http://localhost:8082'
.\mvnw.cmd spring-boot:run
```

La importación `.env` preexistente se conserva para ejecución normal; las pruebas la deshabilitan explícitamente. No copiar credenciales a comandos guardados o documentación. Detener con Ctrl+C deja pendientes recuperables; no ejecutar limpieza de tablas/volúmenes. Para detener envíos sin perder captura, reiniciar Requests con `KAFKA_OUTBOX_WORKER_ENABLED=false` y `KAFKA_PUBLISHER_ENABLED=true`.

## Diagnóstico y recuperación

Consultar outbox por published_at, attempts, available_at y last_error; los logs incluyen eventId, requestId y requestVersion. Una confirmación en log puede preceder al commit: `published_at` persistido es la evidencia del marcador, y `processed_event` en Report confirma la aplicación del consumidor.

- Catalog inaccesible/404: restaurar servicio/dato real; el worker reintenta sin intervención ni edición del JSON.
- Kafka caído: restaurar conectividad/ISR del clúster existente; no marcar manualmente como publicado. El worker continúa solo.
- Base de Requests inaccesible: la transacción falla; no confirmar solicitudes que no se pudieron guardar. Reiniciar después de restablecer conexión.
- Historial bloqueado: seguir la revisión por solicitud anterior; no desbloquear indiscriminadamente.
- JSON preparado: no editarlo ni cambiar eventId aunque el envío haya fallado; puede haber llegado al broker.

No hay limpieza automática: outbox, stream e historial se conservan para trazabilidad. Monitorear tamaño y antigüedad de pendientes. Una política de archivado/purga requiere evaluación separada y nunca debe borrar pendientes o información necesaria para deduplicar/reconstruir.
