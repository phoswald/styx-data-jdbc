package styx.data.jdbc;

import java.util.Optional;

import styx.data.Store;
import styx.data.StoreProvider;
import styx.data.db.DatabaseStore;

public class JdbcStoreProvider implements StoreProvider {

    @Override
    public Optional<Store> openStore(String url) {
        if(url.startsWith("jdbc:")) {
            return Optional.of(new DatabaseStore(new JdbcDatabase(url)));
        } else {
            return Optional.empty();
        }
    }
}
