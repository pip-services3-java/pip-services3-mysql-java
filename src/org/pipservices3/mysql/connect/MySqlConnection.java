package org.pipservices3.mysql.connect;

import java.sql.*;
import java.util.Map;

import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.config.IConfigurable;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;
import org.pipservices3.commons.errors.ConnectionException;
import org.pipservices3.commons.refer.IReferenceable;
import org.pipservices3.commons.refer.IReferences;
import org.pipservices3.commons.refer.ReferenceException;
import org.pipservices3.commons.run.IOpenable;
import org.pipservices3.components.log.CompositeLogger;

public class MySqlConnection implements IReferenceable, IConfigurable, IOpenable {

    private final ConfigParams _defaultConfig = ConfigParams.fromTuples(
            // connections.*
            // credential.*

            "options.connect_timeout", 0,
            "options.idle_timeout", 10000,
            "options.max_pool_size", 3
    );

    /**
     * The logger.
     */
    protected CompositeLogger _logger = new CompositeLogger();
    /**
     * The connection resolver.
     */
    protected MySqlConnectionResolver _connectionResolver = new MySqlConnectionResolver();
    /**
     * The configuration options.
     */
    protected ConfigParams _options = new ConfigParams();

    /**
     * The MySQL connection pool object.
     */
    protected Connection _connection;
    /**
     * The MySQL database name.
     */
    protected String _databaseName;

    @Override
    public void configure(ConfigParams config) throws ConfigException {
        config = config.setDefaults(this._defaultConfig);

        this._connectionResolver.configure(config);

        this._options = this._options.override(config.getSection("options"));
    }

    /**
     * Sets references to dependent components.
     *
     * @param references references to locate the component dependencies.
     */
    @Override
    public void setReferences(IReferences references) throws ReferenceException, ConfigException {
        this._logger.setReferences(references);
        this._connectionResolver.setReferences(references);
    }


    /**
     * Checks if the component is opened.
     *
     * @return true if the component has been opened and false otherwise.
     */
    @Override
    public boolean isOpen() {
        return this._connection != null;
    }

    private String composeUriSettings(String uri) {
        var maxPoolSize = this._options.getAsNullableInteger("max_pool_size");
        var connectTimeoutMS = this._options.getAsNullableInteger("connect_timeout");
        var idleTimeoutMS = this._options.getAsNullableInteger("idle_timeout");

        var settings = Map.of(
                "allowMultiQueries", true,
                "connectionLimit", maxPoolSize,
                "connectTimeout", connectTimeoutMS,
                "insecureAuth", true
//            idleTimeoutMillis: idleTimeoutMS
        );

        StringBuilder params = new StringBuilder();
        for (var key : settings.keySet()) {
            if (params.length() > 0) {
                params.append('&');
            }

            params.append(key);

            var value = settings.get(key);
            if (value != null) {
                params.append("=").append(value);
            }
        }
        if (uri.indexOf('?') < 0)
            uri += '?' + params.toString();
        else
            uri += '&' + params.toString();

        return uri;
    }

    /**
     * Opens the component.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void open(String correlationId) throws ApplicationException {
        var uri = this._connectionResolver.resolve(correlationId);

        this._logger.debug(correlationId, "Connecting to MySQL...");

        try {
            uri = this.composeUriSettings(uri);

            Class.forName("com.mysql.cj.jdbc.Driver");

            this._connection = DriverManager.getConnection("jdbc:" + uri);
            this._databaseName = _connection.getMetaData().getDatabaseProductName();

        } catch (Exception ex) {
            throw new ConnectionException(
                    correlationId,
                    "CONNECT_FAILED",
                    "Connection to MySQL failed"
            ).withCause(ex);
        }
    }

    /**
     * Closes component and frees used resources.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void close(String correlationId) throws ApplicationException {
        if (this._connection == null)
            return;

        try {
            this._connection.close();

            this._logger.debug(correlationId, "Disconnected from MySQL database %s", this._databaseName);

            this._connection = null;
            this._databaseName = null;
        } catch (Exception ex) {
            throw new ConnectionException(
                    correlationId,
                    "DISCONNECT_FAILED",
                    "Disconnect from MySQL failed: "
            ).withCause(ex);
        }
    }

    public Connection getConnection() {
        return this._connection;
    }

    public String getDatabaseName() {
        return this._databaseName;
    }
}
