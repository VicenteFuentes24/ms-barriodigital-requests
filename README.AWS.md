# Requests en AWS

Java 21 y Spring Boot 3.3.6, sin cambios en endpoints, entidades, ownership,
transiciones, eventos ni esquema Supabase. El proyecto Maven esta en
`barriodigitalrequests/`.

- `RABBITMQ_ADDRESSES` acepta `host:puerto,host:puerto`. AWS utiliza las IP privadas
  `10.0.1.62:5672,10.0.2.151:5672`, configurables, nunca las IP publicas.
- Si la lista esta vacia, se conservan `RABBITMQ_HOST=localhost` y `RABBITMQ_PORT=5672`.
- Credenciales: `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD`; vhost `RABBITMQ_VIRTUAL_HOST=barriodigital`.
- Spring AMQP gestiona la recuperacion de conexiones; no se activa otra capa de
  automatic recovery del cliente Java. La lista permite intentar otro nodo,
  pero no replica mensajes ni reintenta de forma duradera una publicacion fallida.
- Se conservan AFTER_COMMIT, mensajes persistentes, confirms correlacionados,
  returns y mandatory. AFTER_COMMIT NO garantiza entrega ante caidas; no hay outbox.
- Supabase sigue usando `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`; JDBC con
  `sslmode=require` o verificacion mas estricta. No se cambian tablas.

El Compose AWS central esta en `infra-barriodigital/aws/compose.apps.yml`:
traduce `REQUESTS_DB_*` y `REQUESTS_RABBITMQ_*` de su `.env.apps` a estas variables.
Usa `JPA_DDL_AUTO=validate`, pool de 3 conexiones y health DB + RabbitMQ.
El esquema debe existir antes del arranque; no se ejecuta update/create en AWS.
No publica 8081 en el host. El BFF llama a `http://requests:8081`.
El Dockerfile local existente se conserva. No se cambian pruebas opt-in existentes.

```powershell
cd barriodigitalrequests
.\mvnw.cmd -B -ntp test
.\mvnw.cmd -B -ntp clean package
```

`RabbitAddressConfigurationTest` verifica valores locales, lista multinodo y
preservacion de confirms/returns/mandatory/topologia centralizada, sin broker real.
`RequestRabbitBrokerIT` requiere un broker dedicado con cola certificate vacia:
NO ejecutarlo sobre la cola AWS compartida ni con Notify consumiendo.
Guia completa: README.AWS.md del repositorio infra-barriodigital.
