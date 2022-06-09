package org.pipservices3.mysql.connect;

import org.junit.Test;

import static org.junit.Assert.*;

import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;

public class MySqlConnectionResolverTest {

    @Test
    public void testConnectionConfig() throws ApplicationException {
        var dbConfig = ConfigParams.fromTuples(
                "connection.host", "localhost",
                "connection.port", 3306,
                "connection.database", "test",
                "connection.ssl", false,
                "credential.username", "mysql",
                "credential.password", "mysql"
        );

        var resolver = new MySqlConnectionResolver();
        resolver.configure(dbConfig);

        var uri = resolver.resolve(null);
        assertNotNull(uri);
        assertEquals("mysql://mysql:mysql@localhost:3306/test?ssl=false", uri);
    }

}
