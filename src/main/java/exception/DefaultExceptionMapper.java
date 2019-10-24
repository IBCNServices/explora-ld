package exception;

import model.ErrorMessage;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.core.Response;

@Provider
public class DefaultExceptionMapper implements ExceptionMapper<Exception> {

    @Override
    public Response toResponse(Exception exception) {
        exception.printStackTrace();
//        Response errorResp = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
//                .entity(new ErrorMessage(e.getMessage(), 400))
//                .build();
//        throw new WebApplicationException(errorResp);
        return Response.serverError().entity(exception.getMessage()).build();
    }
}