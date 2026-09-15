package cl.barriodigital.barriodigitalrequests.service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import cl.barriodigital.barriodigitalrequests.dto.CreateRequestRequest;
import cl.barriodigital.barriodigitalrequests.dto.RequestResponse;
import cl.barriodigital.barriodigitalrequests.dto.UpdateRequestStatusRequest;
import cl.barriodigital.barriodigitalrequests.exception.InvalidRequestFilterException;
import cl.barriodigital.barriodigitalrequests.exception.InvalidStatusTransitionException;
import cl.barriodigital.barriodigitalrequests.exception.RequestNotFoundException;
import cl.barriodigital.barriodigitalrequests.model.RequestEntity;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import cl.barriodigital.barriodigitalrequests.repository.RequestRepository;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@Service
public class RequestService {

    private static final Map<RequestStatus, Set<RequestStatus>> ALLOWED_TRANSITIONS = buildAllowedTransitions();

    private final RequestRepository requestRepository;
    private final RequestMapper requestMapper;

    public RequestService(RequestRepository requestRepository, RequestMapper requestMapper) {
        this.requestRepository = requestRepository;
        this.requestMapper = requestMapper;
    }

    @Transactional
    public RequestResponse createRequest(CreateRequestRequest request, String createdBy) {
        RequestEntity entity = new RequestEntity();
        entity.setProcedureTypeId(request.procedureTypeId().trim());
        entity.setDescription(request.description().trim());
        entity.setStatus(RequestStatus.INGRESADO);
        entity.setCreatedBy(StringUtils.hasText(createdBy) ? createdBy.trim() : null);

        return requestMapper.toResponse(requestRepository.save(entity));
    }

    @Transactional(readOnly = true)
    public RequestResponse getRequestById(Long id, RequestUserContext userContext) {
        RequestEntity entity = requestRepository.findById(id)
                .orElseThrow(() -> new RequestNotFoundException(id));

        if (!userContext.canReadCreatedBy(entity.getCreatedBy())) {
            throw new RequestNotFoundException(id);
        }

        return requestMapper.toResponse(entity);
    }

    @Transactional(readOnly = true)
    public List<RequestResponse> getRequests(RequestStatus status, String from, String to, RequestUserContext userContext) {
        userContext.validateReadAccess();
        Instant fromInstant = parseDateFilter(from, "from");
        Instant toInstant = parseDateFilter(to, "to");

        if (fromInstant != null && toInstant != null && fromInstant.isAfter(toInstant)) {
            throw new InvalidRequestFilterException("El filtro from no puede ser posterior a to.");
        }

        Specification<RequestEntity> specification = Specification.where(null);

        if (status != null) {
            specification = specification.and((root, query, criteriaBuilder) ->
                    criteriaBuilder.equal(root.get("status"), status));
        }

        if (fromInstant != null) {
            specification = specification.and((root, query, criteriaBuilder) ->
                    criteriaBuilder.greaterThanOrEqualTo(root.get("createdAt"), fromInstant));
        }

        if (toInstant != null) {
            specification = specification.and((root, query, criteriaBuilder) ->
                    criteriaBuilder.lessThanOrEqualTo(root.get("createdAt"), toInstant));
        }

        if (!userContext.hasGlobalReadAccess()) {
            String normalizedEmail = userContext.email().trim().toLowerCase(Locale.ROOT);
            specification = specification.and((root, query, criteriaBuilder) ->
                    criteriaBuilder.equal(
                            criteriaBuilder.lower(root.get("createdBy")),
                            normalizedEmail));
        }

        return requestRepository.findAll(specification, Sort.by(Sort.Direction.DESC, "createdAt"))
                .stream()
                .map(requestMapper::toResponse)
                .toList();
    }

    @Transactional
    public RequestResponse updateStatus(Long id, UpdateRequestStatusRequest request) {
        RequestEntity entity = requestRepository.findById(id)
                .orElseThrow(() -> new RequestNotFoundException(id));

        validateTransition(entity.getStatus(), request.status());
        entity.setStatus(request.status());

        return requestMapper.toResponse(requestRepository.save(entity));
    }

    private void validateTransition(RequestStatus currentStatus, RequestStatus newStatus) {
        Set<RequestStatus> allowedNextStatuses = ALLOWED_TRANSITIONS.getOrDefault(currentStatus, Set.of());

        if (!allowedNextStatuses.contains(newStatus)) {
            throw new InvalidStatusTransitionException(currentStatus, newStatus);
        }
    }

    private Instant parseDateFilter(String value, String parameterName) {
        if (!StringUtils.hasText(value)) {
            return null;
        }

        String cleanValue = value.trim();

        try {
            return Instant.parse(cleanValue);
        } catch (DateTimeParseException ignored) {
            try {
                return LocalDate.parse(cleanValue).atStartOfDay().toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException exception) {
                throw new InvalidRequestFilterException(
                        "El filtro " + parameterName + " debe ser una fecha ISO válida.");
            }
        }
    }

    private static Map<RequestStatus, Set<RequestStatus>> buildAllowedTransitions() {
        Map<RequestStatus, Set<RequestStatus>> transitions = new EnumMap<>(RequestStatus.class);
        transitions.put(RequestStatus.INGRESADO, EnumSet.of(RequestStatus.ADMITIDO, RequestStatus.RECHAZADO));
        transitions.put(RequestStatus.ADMITIDO, EnumSet.of(RequestStatus.EN_GESTION, RequestStatus.RECHAZADO));
        transitions.put(RequestStatus.EN_GESTION, EnumSet.of(RequestStatus.EN_TERRENO));
        transitions.put(RequestStatus.EN_TERRENO, EnumSet.of(RequestStatus.RESUELTO));
        transitions.put(RequestStatus.RESUELTO, EnumSet.noneOf(RequestStatus.class));
        transitions.put(RequestStatus.RECHAZADO, EnumSet.noneOf(RequestStatus.class));
        return Map.copyOf(transitions);
    }
}
