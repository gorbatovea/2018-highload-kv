package script;

import ru.mail.polis.KVDao;

public class Script implements IScript {

    @Override
    public String apply(KVDao context) throws Exception {
        context.upsert("1".getBytes(), "1-value".getBytes());
        return new String(context.get("1".getBytes()));
    }

}
