package cl.barriodigital.barriodigitalrequests.exception;

public class RequestNotFoundException extends RuntimeException {

    public RequestNotFoundException(Long id) {
        super("No existe el trámite solicitado: " + id + ".");
    }
}
