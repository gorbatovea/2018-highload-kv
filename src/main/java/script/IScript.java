package script;

import ru.mail.polis.KVDao;

public interface IScript {

    String apply(final KVDao context) throws Exception;

}
