package ru.mail.polis.service;

import one.nio.http.*;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class service extends HttpServer implements KVService {
    private final String ID_PARAM = "id=";
    private final String ENTITY_PATH = "/v0/entity";
    private final String STATUS_PATH = "/v0/status";

    @NotNull
    private final KVDao dao;

    public service(@NotNull HttpServerConfig config, @NotNull KVDao dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    @Path(STATUS_PATH)
    public void status(Request request, HttpSession session) throws IOException {
        if (request.getMethod() == Request.METHOD_GET) {
            session.sendResponse(Response.ok(Response.EMPTY));
        } else {
            session.sendError(Response.METHOD_NOT_ALLOWED, null);
        }
    }

    @Path(ENTITY_PATH)
    public void entity(Request request, HttpSession session) throws IOException {
        try {
            if (request.getParameter(ID_PARAM) == null) {
                session.sendError(Response.BAD_REQUEST, null);
            } else if (request.getParameter(ID_PARAM).isEmpty()) {
                session.sendError(Response.BAD_REQUEST, null);
            }
            switch (request.getMethod()) {
                case Request.METHOD_PUT: {
                    dao.upsert(request.getParameter(ID_PARAM).getBytes(), request.getBody());
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                    return;
                }
                case Request.METHOD_GET: {
                    session.sendResponse(new Response(Response.OK, dao.get(request.getParameter(ID_PARAM).getBytes())));
                    return;
                }
                case Request.METHOD_DELETE: {
                    dao.remove(request.getParameter(ID_PARAM).getBytes());
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                    return;
                }
                default: {
                    session.sendError(Response.METHOD_NOT_ALLOWED, null);
                }
            }
        }catch (NoSuchElementException nSEE) {
            session.sendError(Response.NOT_FOUND, null);
            nSEE.printStackTrace();

        } catch (Exception e) {
            session.sendError(Response.INTERNAL_ERROR, null);
            e.printStackTrace();
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendError(Response.NOT_FOUND, null);
    }
}
