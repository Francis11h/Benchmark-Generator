import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.*;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.stardog.stark.io.RDFFormats;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;

public class StardogConnection {
    private static String url = "http://localhost:5820";
    private static String to = "testDB";
    private static String username = "admin";
    private static String password = "admin";

    public static void main(String[] args) {
        connection();
    }

    public static ConnectionPool connection() {

        try (final AdminConnection aConn = AdminConnectionConfiguration.toServer(url)
                .credentials(username, password)
                .connect()) {

            // A look at what databases are currently in Stardog
            aConn.list().forEach(item -> System.out.println(item));

            // Checks to see if the 'myNewDB' is in Stardog. If it is, we are
            // going to drop it so we are starting fresh
            if (aConn.list().contains(to)) {
                aConn.drop(to);
            }
            // Convenience function for creating a persistent
            // database with all the default settings.
            aConn.disk(to).create();
        }

        ConnectionConfiguration connectionConfig = ConnectionConfiguration
                .to(to)
                .server(url)
                .credentials(username, password);
        ConnectionPool connectionPool = createConnectionPool(connectionConfig);
        return connectionPool;

    }

    /**
     * Now we want to create the configuration for our pool.
     * @param connectionConfig the configuration for the connection pool
     * @return the newly created pool which we will use to get our Connections
     */
    private static ConnectionPool createConnectionPool (ConnectionConfiguration connectionConfig) {
        ConnectionPoolConfig poolConfig = ConnectionPoolConfig
                .using(connectionConfig)
                .minPool(10)
                .maxPool(200)
                .expiration(300, TimeUnit.SECONDS)
                .blockAtCapacity(900, TimeUnit.SECONDS);

        return poolConfig.create();
    }
}
