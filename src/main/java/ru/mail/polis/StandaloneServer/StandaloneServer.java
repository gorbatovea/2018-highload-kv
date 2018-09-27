package ru.mail.polis.StandaloneServer;

import one.nio.http.*;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;

public class StandaloneServer extends HttpServer implements KVService {
    private final KVDao dao;
    public StandaloneServer(HttpServerConfig config, KVDao dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    @Path("/v0/entity/status")
    public void status(Request request, HttpSession session) throws IOException{
        session.sendResponse(Response.ok(Response.EMPTY));
    }
}
