package ru.mail.polis.StandaloneServer;

import one.nio.http.*;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.NoSuchElementException;

public class StandaloneServer extends HttpServer implements KVService {
    private final String ID_PARAM = "id=";
    private final String URL = "/v0/entity";
    private final byte[] EMPTY_BODY = new byte[0];

    private final KVDao dao;

    public StandaloneServer(HttpServerConfig config, KVDao dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    @Path("/v0/status")
    public void status(Request request, HttpSession session) throws IOException {
        if (request.getMethod() == Request.METHOD_GET) {
            session.sendResponse(Response.ok(Response.EMPTY));
        } else {
            session.sendError(Response.METHOD_NOT_ALLOWED, null);
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        try {
            if (request.getPath().equals(URL) && !request.getParameter(ID_PARAM).isEmpty()) {
                switch (request.getMethod()) {
                    case Request.METHOD_PUT: {
                        dao.upsert(request.getParameter(ID_PARAM).getBytes(), request.getBody());
                        session.sendResponse(new Response(Response.CREATED, EMPTY_BODY));
                        System.out.println(Response.CREATED);
                        return;
                    }
                    case Request.METHOD_GET: {
                        session.sendResponse(new Response(Response.OK, dao.get(request.getParameter(ID_PARAM).getBytes())));
                        return;
                    }
                    case Request.METHOD_DELETE: {
                        dao.remove(request.getParameter(ID_PARAM).getBytes());
                        session.sendResponse(new Response(Response.ACCEPTED, EMPTY_BODY));
                        return;
                    }
                    default: {
                        session.sendError(Response.METHOD_NOT_ALLOWED, null);
                    }

                }
            } else{
                session.sendError(Response.BAD_REQUEST, null);
            }
        } catch (NoSuchElementException nSEE) {
            nSEE.printStackTrace();
            session.sendError(Response.NOT_FOUND, null);
        } catch (Exception e) {
            e.printStackTrace();
            session.sendError(Response.INTERNAL_ERROR, null);
        }
    }
}
