package org.pipservices3.mysql.persistence;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.refer.Descriptor;
import org.pipservices3.commons.refer.References;
import org.pipservices3.mysql.connect.MySqlConnection;
import org.pipservices3.mysql.fixtures.DummyPersistenceFixture;

public class DummyMySqlConnectionTest {

    MySqlConnection connection;
    DummyMySqlPersistence persistence;
    DummyPersistenceFixture fixture;

    String mysqlUri = System.getenv("MYSQL_URI");
    String mysqlHost = System.getenv("MYSQL_HOST") != null ? System.getenv("MYSQL_HOST") : "localhost";
    int mysqlPort = System.getenv("MYSQL_PORT") != null ? Integer.parseInt(System.getenv("MYSQL_PORT")) : 3306;
    String mysqlDatabase = System.getenv("MYSQL_DB") != null ? System.getenv("MYSQL_DB") : "test";
    String mysqlUser = System.getenv("MYSQL_USER") != null ? System.getenv("MYSQL_USER") : "mysql";
    String mysqlPassword = System.getenv("MYSQL_PASSWORD") != null ? System.getenv("MYSQL_PASSWORD") : "mysql";

    boolean enabled = false;

    public DummyMySqlConnectionTest() {
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

            persistence = new DummyMySqlPersistence();
            persistence.setReferences(References.fromTuples(
                    new Descriptor("pip-services", "connection", "mysql", "default", "1.0"), connection
            ));

            fixture = new DummyPersistenceFixture(persistence);

            connection.open(null);
            persistence.open(null);
            persistence.clear(null);
        }
    }

    @After
    public void teardown() throws ApplicationException {
        persistence.close(null);
        connection.close(null);
    }

    @Test
    public void testConnection() {
        assertNotNull(connection.getConnection());
        assertNotNull(connection.getDatabaseName());
    }

    @Test
    public void testCrudOperations() {
        fixture.testCrudOperations();
    }

    @Test
    public void testBatchOperations() {
        fixture.testBatchOperations();
    }
}
