package org.pipservices3.mysql.connect;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;

public class MySqlConnectionTest {
    boolean enabled = false;
    MySqlConnection connection;

    String mysqlUri = System.getenv("MYSQL_URI");
    String mysqlHost = System.getenv("MYSQL_HOST") != null ? System.getenv("MYSQL_HOST") : "localhost";
    int mysqlPort = System.getenv("MYSQL_PORT") != null ? Integer.parseInt(System.getenv("MYSQL_PORT")) : 3306;
    String mysqlDatabase = System.getenv("MYSQL_DB") != null ? System.getenv("MYSQL_DB") : "test";
    String mysqlUser = System.getenv("MYSQL_USER") != null ? System.getenv("MYSQL_USER") : "mysql";
    String mysqlPassword = System.getenv("MYSQL_PASSWORD") != null ? System.getenv("MYSQL_PASSWORD") : "mysql";

    public MySqlConnectionTest() {
        if (mysqlUri != null || mysqlHost != null)
            enabled = true;
    }

    @Before
    public void setup() throws ApplicationException {
        if (enabled) {
            var dbConfig = ConfigParams.fromTuples(
                    "connection.uri", mysqlUri,
                    "connection.host", mysqlHost,
                    "connection.port", mysqlPort,
                    "connection.database", mysqlDatabase,
                    "credential.username", mysqlUser,
                    "credential.password", mysqlPassword
            );

            connection = new MySqlConnection();
            connection.configure(dbConfig);

            connection.open(null);
        }
    }

    @After
    public void teardown() throws ApplicationException {
        connection.close(null);
    }

    @Test
    public void testOpenAndClose() {
        assertNotNull(connection.getConnection());
        assertNotNull(connection.getDatabaseName());
    }
}
