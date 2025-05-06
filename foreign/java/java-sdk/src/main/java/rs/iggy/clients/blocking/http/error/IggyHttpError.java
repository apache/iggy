package rs.iggy.clients.blocking.http.error;

public record IggyHttpError(
        String id,
        String code,
        String reason,
        String field
) {
}
