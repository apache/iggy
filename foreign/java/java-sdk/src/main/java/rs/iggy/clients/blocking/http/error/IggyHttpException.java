package rs.iggy.clients.blocking.http.error;

public class IggyHttpException extends RuntimeException {
    private final IggyHttpError error;

    public IggyHttpException(IggyHttpError error) {
        super(error.reason());
        this.error = error;
    }

    public IggyHttpError getError() {
        return error;
    }

}
