package org.pipservices3.mysql.connect;

import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.config.IConfigurable;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;
import org.pipservices3.commons.refer.IReferenceable;
import org.pipservices3.commons.refer.IReferences;
import org.pipservices3.commons.refer.ReferenceException;
import org.pipservices3.components.auth.CredentialParams;
import org.pipservices3.components.auth.CredentialResolver;
import org.pipservices3.components.connect.ConnectionParams;
import org.pipservices3.components.connect.ConnectionResolver;

import java.util.List;
import java.util.Objects;

public class MySqlConnectionResolver implements IReferenceable, IConfigurable {

    /**
     * The connections resolver.
     */
    protected ConnectionResolver _connectionResolver = new ConnectionResolver();
    /**
     * The credentials resolver.
     */
    protected CredentialResolver _credentialResolver = new CredentialResolver();

    /**
     * Sets references to dependent components.
     *
     * @param references references to locate the component dependencies.
     */
    @Override
    public void setReferences(IReferences references) {
        this._connectionResolver.setReferences(references);
        this._credentialResolver.setReferences(references);
    }

    /**
     * Configures component by passing configuration parameters.
     *
     * @param config configuration parameters to be set.
     */
    @Override
    public void configure(ConfigParams config) {
        this._connectionResolver.configure(config);
        this._credentialResolver.configure(config);
    }

    private void validateConnection(String correlationId, ConnectionParams connection) throws ConfigException {
        var uri = connection.getUri();
        if (uri != null) return;

        var host = connection.getHost();
        if (host == null) {
            throw new ConfigException(
                    correlationId,
                    "NO_HOST",
                    "Connection host is not set"
            );
        }

        var port = connection.getPort();
        if (port == 0) {
            throw new ConfigException(
                    correlationId,
                    "NO_PORT",
                    "Connection port is not set"
            );
        }

        var database = connection.getAsNullableString("database");
        if (database == null) {
            throw new ConfigException(
                    correlationId,
                    "NO_DATABASE",
                    "Connection database is not set"
            );
        }
    }

    private void validateConnections(String correlationId, List<ConnectionParams> connections) throws ConfigException {
        if (connections == null || connections.size() == 0) {
            throw new ConfigException(
                    correlationId,
                    "NO_CONNECTION",
                    "Database connection is not set"
            );
        }

        for (var connection : connections)
            this.validateConnection(correlationId, connection);
    }

    private String composeUri(List<ConnectionParams> connections, CredentialParams credential) {
        // If there is a uri then return it immediately
        for (var connection : connections) {
            var uri = connection.getUri();
            if (uri != null) return uri;
        }

        // Define hosts
        StringBuilder hosts = new StringBuilder();
        for (var connection : connections) {
            var host = connection.getHost();
            var port = connection.getPort();

            if (hosts.length() > 0) {
                hosts.append(',');
            }
            hosts.append(host).append(':').append(port);
        }

        // Define database
        var database = "";
        for (var connection : connections) {
            database = !database.isEmpty() ? database : connection.getAsNullableString("database");
        }
        if (database.length() > 0) {
            database = '/' + database;
        }

        // Define authentication part
        var auth = "";
        if (credential != null) {
            var username = credential.getUsername();
            if (username != null) {
                var password = credential.getPassword();
                if (password != null) {
                    auth = username + ':' + password + '@';
                } else {
                    auth = username + '@';
                }
            }
        }

        // Define additional parameters parameters
        var connArr = new ConfigParams[connections.size()];
        for (var i = 0; i < connections.size(); i++)
            connArr[i] = connections.get(i);

        var options = ConfigParams.mergeConfigs(connArr).override(credential);
        options.remove("uri");
        options.remove("host");
        options.remove("port");
        options.remove("database");
        options.remove("username");
        options.remove("password");
        StringBuilder params = new StringBuilder();
        var keys = options.getKeys();
        for (var key : keys) {
            if (params.length() > 0) {
                params.append('&');
            }

            params.append(key);

            var value = options.getAsString(key);
            if (value != null) {
                params.append('=').append(value);
            }
        }
        if (params.length() > 0) {
            params.insert(0, '?');
        }

        // Compose uri
        return "mysql://" + auth + hosts + database + params;
    }

    /**
     * Resolves MongoDB connection URI from connection and credential parameters.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @return a resolved URI.
     */
    public String resolve(String correlationId) throws ApplicationException {
        var connections = this._connectionResolver.resolveAll(correlationId);
        // Validate connections
        this.validateConnections(correlationId, connections);

        var credential = this._credentialResolver.lookup(correlationId);
        // Credentials are not validated right now

        return this.composeUri(connections, credential);
    }
}
