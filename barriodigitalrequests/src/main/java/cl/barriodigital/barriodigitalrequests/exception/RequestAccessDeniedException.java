package cl.barriodigital.barriodigitalrequests.exception;

public class RequestAccessDeniedException extends RuntimeException {

    public RequestAccessDeniedException(String message) {
        super(message);
    }
}
