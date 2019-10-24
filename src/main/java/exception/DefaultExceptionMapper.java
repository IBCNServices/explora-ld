package exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.core.Response;

@Provider
public class DefaultExceptionMapper implements ExceptionMapper<Exception> {

    @Override
    public Response toResponse(Exception exception) {
        exception.printStackTrace();
        return Response.serverError().entity(exception.getMessage()).build();
    }
}