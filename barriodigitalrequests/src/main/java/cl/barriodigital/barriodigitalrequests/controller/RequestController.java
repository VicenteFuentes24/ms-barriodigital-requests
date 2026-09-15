package cl.barriodigital.barriodigitalrequests.controller;

import java.net.URI;
import java.util.List;

import cl.barriodigital.barriodigitalrequests.dto.CreateRequestRequest;
import cl.barriodigital.barriodigitalrequests.dto.RequestResponse;
import cl.barriodigital.barriodigitalrequests.dto.UpdateRequestStatusRequest;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import cl.barriodigital.barriodigitalrequests.service.RequestService;
import cl.barriodigital.barriodigitalrequests.service.RequestUserContext;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/requests")
public class RequestController {

    private final RequestService requestService;

    public RequestController(RequestService requestService) {
        this.requestService = requestService;
    }

    @PostMapping
    public ResponseEntity<RequestResponse> createRequest(
            @Valid @RequestBody CreateRequestRequest request,
            @RequestHeader(value = "X-User-Email", required = false) String userEmail,
            @RequestHeader(value = "X-User-Id", required = false) String userId) {
        RequestResponse response = requestService.createRequest(request, resolveCreatedBy(userEmail, userId));
        return ResponseEntity
                .created(URI.create("/api/requests/" + response.id()))
                .body(response);
    }

    @GetMapping
    public ResponseEntity<List<RequestResponse>> getRequests(
            @RequestParam(required = false) RequestStatus status,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestHeader(value = "X-User-Email", required = false) String userEmail,
            @RequestHeader(value = "X-User-Roles", required = false) String userRoles) {
        return ResponseEntity.ok(requestService.getRequests(
                status,
                from,
                to,
                RequestUserContext.fromHeaders(userEmail, userRoles)));
    }

    @GetMapping("/{id}")
    public ResponseEntity<RequestResponse> getRequestById(
            @PathVariable Long id,
            @RequestHeader(value = "X-User-Email", required = false) String userEmail,
            @RequestHeader(value = "X-User-Roles", required = false) String userRoles) {
        return ResponseEntity.ok(requestService.getRequestById(
                id,
                RequestUserContext.fromHeaders(userEmail, userRoles)));
    }

    @PutMapping("/{id}/status")
    public ResponseEntity<RequestResponse> updateStatus(
            @PathVariable Long id,
            @Valid @RequestBody UpdateRequestStatusRequest request) {
        return ResponseEntity.ok(requestService.updateStatus(id, request));
    }

    private String resolveCreatedBy(String userEmail, String userId) {
        if (StringUtils.hasText(userEmail)) {
            return userEmail;
        }

        if (StringUtils.hasText(userId)) {
            return userId;
        }

        return null;
    }
}
