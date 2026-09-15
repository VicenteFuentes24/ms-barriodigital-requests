package cl.barriodigital.barriodigitalrequests.exception;

import cl.barriodigital.barriodigitalrequests.model.RequestStatus;

public class InvalidStatusTransitionException extends RuntimeException {

    public InvalidStatusTransitionException(RequestStatus currentStatus, RequestStatus newStatus) {
        super("No se puede cambiar el trámite de " + currentStatus + " a " + newStatus + ".");
    }
}
