package org.pipservices3.mysql.persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.mysql.fixtures.Dummy2PersistenceFixture;

public class Dummy2MySqlPersistenceTest {
    Dummy2MySqlPersistence persistence;
    Dummy2PersistenceFixture fixture;

    String mysqlUri = System.getenv("MYSQL_URI");
    String mysqlHost = System.getenv("MYSQL_HOST") != null ? System.getenv("MYSQL_HOST") : "localhost";
    int mysqlPort = System.getenv("MYSQL_PORT") != null ? Integer.parseInt(System.getenv("MYSQL_PORT")) : 3306;
    String mysqlDatabase = System.getenv("MYSQL_DB") != null ? System.getenv("MYSQL_DB") : "test";
    String mysqlUser = System.getenv("MYSQL_USER") != null ? System.getenv("MYSQL_USER") : "mysql";
    String mysqlPassword = System.getenv("MYSQL_PASSWORD") != null ? System.getenv("MYSQL_PASSWORD") : "mysql";

    boolean enabled = false;

    public Dummy2MySqlPersistenceTest() {
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

            persistence = new Dummy2MySqlPersistence();
            persistence.configure(dbConfig);

            fixture = new Dummy2PersistenceFixture(persistence);

            persistence.open(null);
            persistence.clear(null);
        }
    }

    @After
    public void teardown() throws ApplicationException {
        if (enabled)
            persistence.close(null);
    }

    @Test
    public void testCrudOperations() {
        if (enabled)
            fixture.testCrudOperations();
    }

    @Test
    public void testBatchOperations() {
        if (enabled)
            fixture.testBatchOperations();
    }
}
