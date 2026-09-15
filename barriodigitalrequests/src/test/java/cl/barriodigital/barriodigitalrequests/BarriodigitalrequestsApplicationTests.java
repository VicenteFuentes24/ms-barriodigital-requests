package cl.barriodigital.barriodigitalrequests;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.equalTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.barriodigital.barriodigitalrequests.repository.RequestRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:requestsdb;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect"
})
@AutoConfigureMockMvc
class BarriodigitalrequestsApplicationTests {

    private static final String CLIENTE_A = "cliente.a@test.cl";
    private static final String CLIENTE_B = "cliente.b@test.cl";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RequestRepository requestRepository;

    @BeforeEach
    void cleanDatabase() {
        requestRepository.deleteAll();
    }

    @Test
    void createRequestStartsWithIngresado() throws Exception {
        mockMvc.perform(post("/api/requests")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createBody()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.procedureTypeId").value("procedure-1"))
                .andExpect(jsonPath("$.description").value("Solicitud de prueba para el barrio"))
                .andExpect(jsonPath("$.status").value("INGRESADO"));
    }

    @Test
    void getExistingRequestReturns200() throws Exception {
        Long id = createRequest(CLIENTE_A);

        mockMvc.perform(withUserHeaders(get("/api/requests/{id}", id), CLIENTE_A, "Admin"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.status").value("INGRESADO"));
    }

    @Test
    void getMissingRequestReturns404() throws Exception {
        mockMvc.perform(withUserHeaders(get("/api/requests/{id}", 999L), CLIENTE_A, "Admin"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void transitionIngresadoToAdmitidoIsValid() throws Exception {
        Long id = createRequest();

        updateStatus(id, "ADMITIDO")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ADMITIDO"));
    }

    @Test
    void transitionIngresadoToEnTerrenoIsInvalid() throws Exception {
        Long id = createRequest();

        updateStatus(id, "EN_TERRENO")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("No se puede cambiar el trámite de INGRESADO a EN_TERRENO."));
    }

    @Test
    void transitionAdmitidoToEnGestionIsValid() throws Exception {
        Long id = createRequest();

        updateStatus(id, "ADMITIDO").andExpect(status().isOk());
        updateStatus(id, "EN_GESTION")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("EN_GESTION"));
    }

    @Test
    void finalStatusCannotMoveBackToAdmitido() throws Exception {
        Long id = createRequest();

        updateStatus(id, "ADMITIDO").andExpect(status().isOk());
        updateStatus(id, "EN_GESTION").andExpect(status().isOk());
        updateStatus(id, "EN_TERRENO").andExpect(status().isOk());
        updateStatus(id, "RESUELTO").andExpect(status().isOk());

        updateStatus(id, "ADMITIDO")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("No se puede cambiar el trámite de RESUELTO a ADMITIDO."));
    }

    @Test
    void createRequestWithoutDescriptionReturns400() throws Exception {
        mockMvc.perform(post("/api/requests")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "procedureTypeId": "procedure-1"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void adminGetRequestsSeesRequestsFromSeveralUsers() throws Exception {
        createRequest(CLIENTE_A);
        createRequest(CLIENTE_B);

        mockMvc.perform(withUserHeaders(get("/api/requests"), "admin@test.cl", "Admin"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                ;
    }

    @Test
    void operadorGetRequestsSeesRequestsFromSeveralUsers() throws Exception {
        createRequest(CLIENTE_A);
        createRequest(CLIENTE_B);

        mockMvc.perform(withUserHeaders(get("/api/requests"), "operador@test.cl", "Operador"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)));
    }

    @Test
    void clienteAGetRequestsOnlySeesOwnRequests() throws Exception {
        createRequest(CLIENTE_A);
        createRequest(CLIENTE_A.toUpperCase());
        createRequest(CLIENTE_B);

        mockMvc.perform(withUserHeaders(get("/api/requests"), CLIENTE_A, "Cliente"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[*].createdBy", everyItem(org.hamcrest.Matchers.equalToIgnoringCase(CLIENTE_A))));
    }

    @Test
    void clienteBGetRequestsOnlySeesOwnRequests() throws Exception {
        createRequest(CLIENTE_A);
        createRequest(CLIENTE_B);
        createRequest(CLIENTE_B);

        mockMvc.perform(withUserHeaders(get("/api/requests"), CLIENTE_B, "Cliente"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[*].createdBy", everyItem(equalTo(CLIENTE_B))));
    }

    @Test
    void clienteAWithStatusFilterOnlySeesOwnRequestsWithThatStatus() throws Exception {
        Long ownIngresado = createRequest(CLIENTE_A);
        Long ownAdmitido = createRequest(CLIENTE_A);
        createRequest(CLIENTE_B);
        updateStatus(ownAdmitido, "ADMITIDO").andExpect(status().isOk());

        mockMvc.perform(withUserHeaders(get("/api/requests?status=INGRESADO"), CLIENTE_A, "Cliente"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].id").value(ownIngresado))
                .andExpect(jsonPath("$[0].createdBy").value(CLIENTE_A))
                .andExpect(jsonPath("$[0].status").value("INGRESADO"));
    }

    @Test
    void clienteAGetOwnRequestByIdReturns200() throws Exception {
        Long id = createRequest(CLIENTE_A);

        mockMvc.perform(withUserHeaders(get("/api/requests/{id}", id), CLIENTE_A, "Cliente"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.createdBy").value(CLIENTE_A));
    }

    @Test
    void clienteAGetOtherClientRequestByIdReturns404() throws Exception {
        Long id = createRequest(CLIENTE_B);

        mockMvc.perform(withUserHeaders(get("/api/requests/{id}", id), CLIENTE_A, "Cliente"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void adminGetOtherClientRequestByIdReturns200() throws Exception {
        Long id = createRequest(CLIENTE_B);

        mockMvc.perform(withUserHeaders(get("/api/requests/{id}", id), "admin@test.cl", "Admin"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.createdBy").value(CLIENTE_B));
    }

    @Test
    void clienteWithoutEmailReturns403() throws Exception {
        createRequest(CLIENTE_A);

        mockMvc.perform(get("/api/requests").header("X-User-Roles", "Cliente"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403))
                .andExpect(jsonPath("$.message").value("No se pudo determinar el usuario autenticado."));
    }

    @Test
    void adminClienteRolesHaveGlobalAccess() throws Exception {
        createRequest(CLIENTE_A);
        createRequest(CLIENTE_B);

        mockMvc.perform(withUserHeaders(get("/api/requests"), CLIENTE_A, "Admin,Cliente"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)));
    }

    @Test
    void auditorRoleCannotReadRequestsDirectly() throws Exception {
        createRequest(CLIENTE_A);

        mockMvc.perform(withUserHeaders(get("/api/requests"), "auditor@test.cl", "Auditor"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403));
    }

    private Long createRequest() throws Exception {
        return createRequest(null);
    }

    private Long createRequest(String userEmail) throws Exception {
        MockHttpServletRequestBuilder request = post("/api/requests")
                .contentType(MediaType.APPLICATION_JSON)
                .content(createBody());

        if (userEmail != null) {
            request.header("X-User-Email", userEmail);
        }

        MvcResult result = mockMvc.perform(request)
                .andExpect(status().isCreated())
                .andReturn();

        JsonNode response = objectMapper.readTree(result.getResponse().getContentAsString());
        return response.get("id").asLong();
    }

    private ResultActionsWrapper updateStatus(Long id, String status) throws Exception {
        return new ResultActionsWrapper(mockMvc.perform(put("/api/requests/{id}/status", id)
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {
                          "status": "%s",
                          "comment": "Comentario de prueba"
                        }
                        """.formatted(status))));
    }

    private MockHttpServletRequestBuilder withUserHeaders(
            MockHttpServletRequestBuilder request,
            String userEmail,
            String userRoles) {
        return request
                .header("X-User-Email", userEmail)
                .header("X-User-Roles", userRoles);
    }

    private String createBody() {
        return """
                {
                  "procedureTypeId": "procedure-1",
                  "description": "Solicitud de prueba para el barrio"
                }
                """;
    }

    private record ResultActionsWrapper(org.springframework.test.web.servlet.ResultActions delegate) {
        ResultActionsWrapper andExpect(org.springframework.test.web.servlet.ResultMatcher matcher) throws Exception {
            delegate.andExpect(matcher);
            return this;
        }
    }
}


