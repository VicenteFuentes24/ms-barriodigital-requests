# Requests - publicacion RabbitMQ EP3/EP4

Se conserva Java 21, Spring Boot 3.3.6, JPA y la conexion PostgreSQL/Supabase.
La nueva dependencia es `spring-boot-starter-amqp`, administrada por Spring Boot.
Los endpoints, ownership, roles y transiciones mantienen su contrato actual.

## Configuracion

Variables documentadas tambien en `barriodigitalrequests/.env.example`:

| Variable | Valor por defecto / uso |
| --- | --- |
| `RABBITMQ_HOST` | `localhost` en el host; `rabbitmq` dentro de Docker |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USERNAME` | Sin valor por defecto; usuario del broker local |
| `RABBITMQ_PASSWORD` | Sin valor por defecto; password del broker local |
| `RABBITMQ_VIRTUAL_HOST` | `barriodigital` |
| `RABBITMQ_CONNECTION_TIMEOUT_MS` | `3000` |
| `RABBITMQ_CONFIRM_TIMEOUT_MS` | `5000` por comando |
| `RABBITMQ_HEALTH_ENABLED` | `false`; evita cambiar el healthcheck HTTP anterior |

Las variables del broker de infraestructura se llaman `RABBITMQ_DEFAULT_USER` y
`RABBITMQ_DEFAULT_PASS`; Requests usa los mismos valores mediante
`RABBITMQ_USERNAME` y `RABBITMQ_PASSWORD`. No hay credenciales reales versionadas.
El archivo `.env` real existente no fue modificado. Las variables DB permanecen
como estaban y no se agregan tablas ni migraciones.

Iniciar RabbitMQ e importar `definitions.json` siguiendo el README de
`infra-barriodigital`. Despues, para ejecutar Requests desde PowerShell:

```powershell
Set-Location 'G:\Desarrollo EP3-EP4\ms-barriodigital-requests\barriodigitalrequests'
$rabbitCredential = Get-Credential -Message 'Credenciales del RabbitMQ local ya iniciado'
$env:RABBITMQ_HOST = 'localhost'
$env:RABBITMQ_USERNAME = $rabbitCredential.UserName
$env:RABBITMQ_PASSWORD = $rabbitCredential.GetNetworkCredential().Password
$env:RABBITMQ_VIRTUAL_HOST = 'barriodigital'
.\mvnw.cmd spring-boot:run
```

En una futura integracion Docker usar `RABBITMQ_HOST=rabbitmq`, la red compartida
y las mismas credenciales/vhost. El Compose existente no se modifica en esta fase.

## Topologia centralizada

`RequestsRabbitConfig` define un bean `Declarables` con los objetos Queue,
Exchange y Binding del contrato: tres exchanges, seis colas y nueve bindings.
Sus nombres, tipos, durabilidad y `x-queue-type=classic` coinciden con
`infra-barriodigital/rabbitmq/definitions.json`.

`spring.rabbitmq.dynamic=false` evita crear el RabbitAdmin de Boot. Ademas,
todos los objetos tienen `shouldDeclare=false`, por lo que tampoco los declara
un RabbitAdmin agregado posteriormente. Solo infraestructura crea la topologia
y administra las politicas DLX; las colas Java no incluyen argumentos DLX.

## Flujo y contrato

`RequestService` guarda la entidad y publica un evento de Spring dentro de la
transaccion. `RequestEventFactory` crea una copia inmutable con identificadores
y timestamp. `RequestEventPublisher` recibe ese evento solo en `AFTER_COMMIT`;
un rollback, un fallo de persistencia o una transicion invalida no envia mensajes.
Un evento publicado sin transaccion tampoco activa el listener.

Todos los comandos usan `cmd.direct`:

| Operacion / nuevo estado | Routing key | Cola |
| --- | --- | --- |
| Creacion / `INGRESADO` | `certificate.gen` | `q.cmd.certificate` |
| Cambio a `ADMITIDO` | `email.send` | `q.cmd.email` |
| Cambio a `EN_TERRENO` | `crew.ticket` | `q.cmd.crew` |
| Cambio a `RESUELTO` | `email.send` y `certificate.gen` | email y certificate |

`EN_GESTION` y `RECHAZADO` no generan comandos RabbitMQ en esta fase.

El JSON contiene `type`, `eventId`, `timestamp`, `traceId`, `correlationId`,
`requestId`, `procedureTypeId`, `status` y `createdBy`.
`type` es `REQUEST_CREATED` o `REQUEST_STATUS_CHANGED`; `eventId` es UUID y
`timestamp` es un Instant UTC serializado como texto ISO-8601. No se envia la
descripcion libre, comentarios, roles, JWT ni secretos.
`createdBy` conserva el valor actual: puede ser correo, identificador o null.
Un futuro consumidor de email tendra que resolver un destinatario si no es correo.
Los consumidores pueden recuperar detalles usando `requestId` cuando sea necesario.

El trace usa MDC `traceId` cuando existe; en su defecto acepta `X-Trace-Id` o
genera UUID. `X-Correlation-Id` se propaga o toma el trace como valor por defecto.
Los headers aceptados contienen solo letras, numeros, punto, guion o guion bajo,
con longitud maxima 128. Esta metadata no se utiliza para autorizacion.

Los dos comandos de `RESUELTO` comparten el mismo evento y eventId. La correlation
del confirm es independiente por envio. En AMQP se envia `messageId=eventId`,
`correlationId`, timestamp, header `traceId`, JSON y `delivery_mode=2`.
Se elimina el header Java `__TypeId__` para permitir consumidores independientes.

## Confirmaciones y fallos

Se habilitan confirms correlacionados, returns y envio obligatorio (`mandatory`).
El productor espera el confirm con timeout y comprueba si el broker devolvio el
mensaje por no encontrar una cola. Un ACK con return no se cuenta como exito.
La publicacion se realiza sin reintentos automaticos en esta fase.

Se registran contadores Micrometer:

- `barriodigital.requests.events.published`, etiquetado por `routing_key`.
- `barriodigital.requests.events.failed`, etiquetado por `routing_key` y `reason`.

Los motivos son `send_error`, `nack`, `unroutable`, `confirm_timeout`,
`confirm_error` e `interrupted`. El log de fallo contiene eventId, requestId,
routing key y motivo; no imprime payloads, credenciales ni excepciones sensibles.
En `RESUELTO` se intenta el segundo comando aunque falle el primero.

La solicitud ya esta confirmada en la BD cuando se publica. Un fallo del broker
no revierte ese guardado ni convierte la respuesta HTTP correcta en un 500.
El tiempo de publicacion puede aumentar la latencia HTTP hasta los limites
configurados. Los contadores siguen internos; no se exponen endpoints Actuator
nuevos. La opcion de health RabbitMQ se puede habilitar cuando se integre su
readiness al despliegue; por defecto se conserva el healthcheck DB/HTTP anterior.

**Limite de entrega:** AFTER_COMMIT evita publicar operaciones revertidas, pero
no hace atomicos PostgreSQL y RabbitMQ. Una caida entre commit y envio puede
perder el evento. Un timeout puede significar entrega incierta, y una repeticion
manual puede duplicarlo. Un confirm acredita recepcion del broker, no consumo.

Un outbox transaccional requeriria una tabla de eventos pendientes, escritura en
la misma transaccion del tramite, un publicador con reintentos, seguimiento de
estado y limpieza. No se implementa sin autorizacion para ese cambio de BD.
Hasta entonces, los fallos requieren observacion y recuperacion operativa;
no existe reintento duradero automatico. Los futuros consumidores deben ser
idempotentes por eventId y comando/routing key. No se crea notify ni consumidores.

## Pruebas

```powershell
Set-Location 'G:\Desarrollo EP3-EP4\ms-barriodigital-requests\barriodigitalrequests'
.\mvnw.cmd test
.\mvnw.cmd clean package
```

Las pruebas normales usan H2 y mocks del transporte solo en tests. Cubren
contrato HTTP existente, ownership, transiciones, publicacion posterior al commit,
rollback, fallo de BD, datos del evento, JSON persistente, confirms, returns y
fallos del broker. No dependen de Entra, Supabase ni RabbitMQ real.

Prueba opt-in con un broker dedicado y `q.cmd.certificate` vacia:

```powershell
$env:RABBITMQ_INTEGRATION_TEST = 'true'
# Definir RABBITMQ_HOST/PORT/USERNAME/PASSWORD/VIRTUAL_HOST del broker dedicado.
.\mvnw.cmd '-Dtest=RequestRabbitBrokerIT' test
Remove-Item Env:RABBITMQ_INTEGRATION_TEST
```

Ese test usa H2 para no tocar Supabase, pero el RequestService, el listener,
RabbitTemplate y el broker son reales. Crea un tramite de prueba y consume el
mensaje de `q.cmd.certificate`, verificando payload y propiedades AMQP. Por eso
debe ejecutarse en un broker aislado; no forma parte de `mvn test` por defecto.

## Verificar en Management

En http://localhost:15672 seleccionar vhost `barriodigital`, cola
`q.cmd.certificate` y revisar un mensaje creado mediante POST `/api/requests`.
Para inspeccion HTTP desde PowerShell conservando el mensaje en la cola:

```powershell
$credential = Get-Credential -Message 'Credenciales del broker local'
$basic = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(
    $credential.UserName + ':' + $credential.GetNetworkCredential().Password))
$headers = @{ Authorization = 'Basic ' + $basic }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:15672/api/queues/barriodigital/q.cmd.certificate/get' -Headers $headers -ContentType 'application/json' -Body '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto"}'
```

Este comando devuelve y reencola el mensaje; no es un consumidor de produccion.
El test opt-in si consume su mensaje aislado. La inspeccion API requiere permisos
Management del usuario local. Los consumidores de la siguiente fase usaran AMQP.

Referencias: [eventos transaccionales Spring](https://docs.spring.io/spring-framework/reference/data-access/transaction/event.html)
y [confirms y returns Spring AMQP 3.1](https://docs.spring.io/spring-amqp/docs/3.1.0/reference/html/).
