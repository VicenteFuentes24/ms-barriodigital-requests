package cl.barriodigital.barriodigitalrequests.service;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import cl.barriodigital.barriodigitalrequests.exception.RequestAccessDeniedException;
import org.springframework.util.StringUtils;

public record RequestUserContext(String email, Set<UserRole> roles) {

    public static RequestUserContext fromHeaders(String userEmail, String userRoles) {
        String normalizedEmail = StringUtils.hasText(userEmail) ? userEmail.trim() : null;
        return new RequestUserContext(normalizedEmail, parseRoles(userRoles));
    }

    public boolean hasGlobalReadAccess() {
        return roles.contains(UserRole.ADMIN) || roles.contains(UserRole.OPERADOR);
    }

    public boolean isCliente() {
        return roles.contains(UserRole.CLIENTE);
    }

    public void validateReadAccess() {
        if (hasGlobalReadAccess()) {
            return;
        }

        if (!isCliente()) {
            throw new RequestAccessDeniedException("No tienes permisos para consultar trámites.");
        }

        if (!StringUtils.hasText(email)) {
            throw new RequestAccessDeniedException("No se pudo determinar el usuario autenticado.");
        }
    }

    public boolean canReadCreatedBy(String createdBy) {
        validateReadAccess();

        if (hasGlobalReadAccess()) {
            return true;
        }

        return StringUtils.hasText(createdBy)
                && StringUtils.hasText(email)
                && createdBy.trim().equalsIgnoreCase(email.trim());
    }

    private static Set<UserRole> parseRoles(String userRoles) {
        if (!StringUtils.hasText(userRoles)) {
            return Set.of();
        }

        return Arrays.stream(userRoles.split(","))
                .map(String::trim)
                .filter(StringUtils::hasText)
                .map(UserRole::fromHeaderValue)
                .filter(role -> role != null)
                .collect(Collectors.toUnmodifiableSet());
    }

    public enum UserRole {
        ADMIN("Admin"),
        OPERADOR("Operador"),
        CLIENTE("Cliente"),
        AUDITOR("Auditor");

        private final String headerValue;

        UserRole(String headerValue) {
            this.headerValue = headerValue;
        }

        static UserRole fromHeaderValue(String value) {
            for (UserRole role : values()) {
                if (role.headerValue.equalsIgnoreCase(value)) {
                    return role;
                }
            }

            return null;
        }
    }
}
