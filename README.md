# ms-barriodigital-requests

Microservicio de trámites de BarrioDigital. Gestiona la creación, consulta, filtrado y cambio de estado de solicitudes comunales.

## Stack

- Java 21
- Spring Boot 3.3.6
- Spring Web
- Spring Data JPA
- PostgreSQL
- Bean Validation
- Actuator
- H2 para pruebas automatizadas

## Responsabilidad

Este servicio contiene la lógica de negocio de los trámites. No se expone directamente al frontend en el despliegue con Docker Compose: recibe peticiones desde el BFF por la red interna.

```text
Frontend -> API Gateway -> BFF -> Requests -> PostgreSQL
```

## Variables de entorno

Crear `.env` dentro de `barriodigitalrequests/` a partir de `.env.example`:

```env
SERVER_PORT=8081
DB_URL=jdbc:postgresql://YOUR_HOST:5432/YOUR_DATABASE
DB_USERNAME=YOUR_DATABASE_USERNAME
DB_PASSWORD=YOUR_DATABASE_PASSWORD
JPA_DDL_AUTO=update
```

El archivo `.env` no debe subirse al repositorio.

## Endpoints

### Crear trámite

```http
POST /api/requests
```

Ejemplo de body:

```json
{
  "procedureTypeId": "1",
  "description": "Descripción de la solicitud"
}
```

El trámite se crea inicialmente en estado `INGRESADO`.

### Listar trámites

```http
GET /api/requests
```

Filtros opcionales:

```text
status
from
to
```

Ejemplo:

```text
GET /api/requests?status=INGRESADO&from=2026-09-01&to=2026-09-30
```

### Obtener detalle

```http
GET /api/requests/{id}
```

### Cambiar estado

```http
PUT /api/requests/{id}/status
```

Body:

```json
{
  "status": "ADMITIDO",
  "comment": "Comentario opcional"
}
```

La autorización para usar este endpoint se aplica en el BFF.

## Estados y transiciones

```text
INGRESADO
  |--> ADMITIDO --> EN_GESTION --> EN_TERRENO --> RESUELTO
  |       |
  |       +--> RECHAZADO
  +--> RECHAZADO
```

Transiciones válidas implementadas:

- `INGRESADO -> ADMITIDO`
- `INGRESADO -> RECHAZADO`
- `ADMITIDO -> EN_GESTION`
- `ADMITIDO -> RECHAZADO`
- `EN_GESTION -> EN_TERRENO`
- `EN_TERRENO -> RESUELTO`

`RESUELTO` y `RECHAZADO` son estados finales.

## Acceso según usuario

El BFF obtiene la identidad desde el JWT y envía headers internos:

```text
X-User-Email
X-User-Id
X-User-Roles
```

Requests usa esos datos para la lectura:

- Admin: puede consultar todos los trámites.
- Operador: puede consultar todos los trámites.
- Cliente: solo puede consultar trámites cuyo `createdBy` corresponde a su email.
- Auditor: no tiene acceso de lectura a Requests.

Si un Cliente intenta consultar el ID de un trámite de otro usuario, el servicio responde `404` para no revelar su existencia.

Al crear un trámite, `createdBy` se obtiene del header interno generado por el BFF; el frontend no envía ese dato en el body.

## Estructura

```text
src/main/java/.../
├── controller/   # endpoints HTTP
├── dto/          # contratos de entrada y salida
├── exception/    # errores del dominio
├── model/        # entidad y estados
├── repository/   # acceso JPA
└── service/      # reglas de negocio y control de acceso
```

## Ejecución local

```powershell
cd .\barriodigitalrequests
.\mvnw.cmd spring-boot:run
```

Servicio: `http://localhost:8081`

Healthcheck:

```text
GET /actuator/health
```

## Tests

```powershell
cd .\barriodigitalrequests
.\mvnw.cmd test
.\mvnw.cmd clean package
```

Las pruebas cubren creación, consultas, validaciones, transiciones de estado, filtros y separación de datos por Cliente.
