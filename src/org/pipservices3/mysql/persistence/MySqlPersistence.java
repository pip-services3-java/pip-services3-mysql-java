package org.pipservices3.mysql.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.config.IConfigurable;
import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.commons.data.DataPage;
import org.pipservices3.commons.data.PagingParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;
import org.pipservices3.commons.errors.ConnectionException;
import org.pipservices3.commons.errors.InvalidStateException;
import org.pipservices3.commons.random.RandomInteger;
import org.pipservices3.commons.refer.*;
import org.pipservices3.commons.run.ICleanable;
import org.pipservices3.commons.run.IOpenable;
import org.pipservices3.components.log.CompositeLogger;
import org.pipservices3.mysql.connect.MySqlConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MySqlPersistence<T> implements IReferenceable, IUnreferenceable, IConfigurable, IOpenable, ICleanable {

    private static final ConfigParams _defaultConfig = ConfigParams.fromTuples(
            "table", null,
            "schema", null,
            "dependencies.connection", "*:connection:mysql:*:1.0",

            // connections.*
            // credential.*

            "options.max_pool_size", 2,
            "options.keep_alive", 1,
            "options.connect_timeout", 5000,
            "options.auto_reconnect", true,
            "options.max_page_size", 100,
            "options.debug", true
    );

    private ConfigParams _config;
    private IReferences _references;
    private boolean _opened = false;
    private boolean _localConnection;
    private List<String> _schemaStatements = new ArrayList<>();

    /**
     * The dependency resolver.
     */
    protected DependencyResolver _dependencyResolver = new DependencyResolver(MySqlPersistence._defaultConfig);
    /**
     * The logger.
     */
    protected CompositeLogger _logger = new CompositeLogger();

    /**
     * The MySQL connection component.
     */
    protected MySqlConnection _connection;

    /**
     * The MySQL connection pool object.
     */
    protected Connection _client;
    /**
     * The MySQL database name.
     */
    protected String _databaseName;
    /**
     * The MySQL table object.
     */
    protected String _tableName;
    /**
     * The MySQL schema object.
     */
    protected String _schemaName;
    /**
     * Max number of objects in data pages
     */
    protected int _maxPageSize = 100;

    /**
     * The default class to cast any documents returned from the database into
     */
    protected Class<T> _documentClass;

    /**
     * Creates a new instance of the persistence component.
     *
     * @param tableName  (optional) a table name.
     * @param schemaName (optional) a schema name.
     */
    public MySqlPersistence(Class<T> documentClass, String tableName, String schemaName) {
        this._tableName = tableName;
        this._schemaName = schemaName;
        this._documentClass = documentClass;
    }

    public MySqlPersistence(Class<T> documentClass) {
        this._documentClass = documentClass;
    }

    /**
     * Configures component by passing configuration parameters.
     *
     * @param config configuration parameters to be set.
     */
    @Override
    public void configure(ConfigParams config) throws ConfigException {
        config = config.setDefaults(MySqlPersistence._defaultConfig);
        this._config = config;

        this._dependencyResolver.configure(config);

        this._tableName = config.getAsStringWithDefault("collection", this._tableName);
        this._tableName = config.getAsStringWithDefault("table", this._tableName);
        this._schemaName = config.getAsStringWithDefault("schema", this._schemaName);
        this._maxPageSize = config.getAsIntegerWithDefault("options.max_page_size", this._maxPageSize);
    }

    /**
     * Sets references to dependent components.
     *
     * @param references references to locate the component dependencies.
     */
    @Override
    public void setReferences(IReferences references) throws ReferenceException, ConfigException {
        this._references = references;
        this._logger.setReferences(references);

        // Get connection
        this._dependencyResolver.setReferences(references);
        this._connection = this._dependencyResolver.getOneOptional(MySqlConnection.class, "connection");
        // Or create a local one
        if (this._connection == null) {
            this._connection = this.createConnection();
            this._localConnection = true;
        } else {
            this._localConnection = false;
        }
    }

    /**
     * Unsets (clears) previously set references to dependent components.
     */
    @Override
    public void unsetReferences() {
        this._connection = null;
    }

    private MySqlConnection createConnection() throws ConfigException, ReferenceException {
        var connection = new MySqlConnection();

        if (this._config != null)
            connection.configure(this._config);

        if (this._references != null)
            connection.setReferences(this._references);

        return connection;
    }

    /**
     * Adds index definition to create it on opening
     *
     * @param keys    index keys (fields)
     * @param options index options
     */
    protected void ensureIndex(String name, Map<String, Object> keys, Map<String, Object> options) {
        var builder = "CREATE";
        options = options != null ? options : Map.of();

        if (options.getOrDefault("unique", null) != null)
            builder += " UNIQUE";

        var indexName = this.quoteIdentifier(name);
        if (this._schemaName != null)
            indexName = this.quoteIdentifier(this._schemaName) + "." + indexName;

        builder += " INDEX " + indexName + " ON " + this.quotedTableName();

        if (options.get("type") != null) {
            builder += " " + options.get("type");
        }

        StringBuilder fields = new StringBuilder();
        for (var key : keys.keySet()) {
            if (!fields.toString().equals("")) fields.append(", ");
            fields.append(this.quoteIdentifier(key));
            var asc = keys.get(key);
            if (!Boolean.parseBoolean(asc.toString())) fields.append(" DESC");
        }

        builder += "(" + fields + ")";

        this.ensureSchema(builder);
    }

    /**
     * Adds a statement to schema definition
     *
     * @param schemaStatement a statement to be added to the schema
     */
    protected void ensureSchema(String schemaStatement) {
        this._schemaStatements.add(schemaStatement);
    }

    /**
     * Clears all auto-created objects
     */
    protected void clearSchema() {
        this._schemaStatements = new ArrayList<>();
    }

    /**
     * Defines database schema via auto create objects or convenience methods.
     */
    protected void defineSchema() {
        // Todo: override in child classes
        this.clearSchema();
    }

    /**
     * Converts object value from internal to public format.
     *
     * @param value an object in internal format to convert.
     * @return converted object in public format.
     */
    protected T convertToPublic(Map<String, Object> value) {
        try {
            if (value != null && !value.isEmpty())
                return JsonConverter.fromJson(_documentClass, JsonConverter.toJson(value));
            return null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Convert object value from public to internal format.
     *
     * @param value an object in public format to convert.
     * @return converted object in internal format.
     */
    protected Map<String, Object> convertFromPublic(Object value) {
        try {
            if (value != null)
                return JsonConverter.toMap(JsonConverter.toJson(value));
            return null;
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected String quoteIdentifier(String value) {
        if (value == null || value.equals("")) return value;

        if (value.indexOf(0) == '`') return value;

        return '`' + value + '`';
    }

    protected String quotedTableName() {
        if (this._tableName == null)
            return null;

        var builder = this.quoteIdentifier(this._tableName);

        if (this._schemaName != null)
            builder = this.quoteIdentifier(this._schemaName) + "." + builder;

        return builder;
    }

    /**
     * Checks if the component is opened.
     *
     * @return true if the component has been opened and false otherwise.
     */
    @Override
    public boolean isOpen() {
        return this._opened;
    }

    /**
     * Opens the component.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void open(String correlationId) throws ApplicationException {
        if (this._opened) {
            return;
        }

        if (this._connection == null) {
            this._connection = this.createConnection();
            this._localConnection = true;
        }

        if (this._localConnection)
            this._connection.open(correlationId);


        if (!this._connection.isOpen()) {
            throw new ConnectionException(
                    correlationId,
                    "CONNECT_FAILED",
                    "MySQL connection is not opened"
            );
        }

        this._opened = false;

        this._client = this._connection.getConnection();
        this._databaseName = this._connection.getDatabaseName();

        // Define database schema
        this.defineSchema();

        try {
            // Recreate objects
            this.createSchema(correlationId);

            this._opened = true;

            this._logger.debug(correlationId, "Connected to MySQL database %s, collection %s",
                    this._databaseName, this._tableName);
        } catch (Exception ex) {
            this._client = null;
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
    public void close(String correlationId) throws ApplicationException {
        if (!this._opened)
            return;

        if (this._connection == null) {
            throw new InvalidStateException(
                    correlationId,
                    "NO_CONNECTION",
                    "MySql connection is missing"
            );
        }

        if (this._localConnection)
            this._connection.close(correlationId);

        this._opened = false;
        this._client = null;
    }

    /**
     * Clears component state.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void clear(String correlationId) {
        // Return error if collection is not set
        if (this._tableName == null) {
            try {
                throw new Exception("Table name is not defined");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        var query = "DELETE FROM " + this.quotedTableName();

        try {
            var stmt = _client.createStatement();
            stmt.execute(query);

            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void createSchema(String correlationId) {
        if (this._schemaStatements == null || this._schemaStatements.size() == 0)
            return;

        // Check if table exist to determine weither to auto create objects
        // Todo: include schema
        var query = "SHOW TABLES LIKE '" + this._tableName + "'";

        Statement stmt;
        ResultSet rs;

        try {
            stmt = this._client.createStatement();
            rs = stmt.executeQuery(query);

            var exist = false;

            // If table already exists then exit
            while (rs.next()) {
                if (rs.getString(1).equals(_tableName)) {
                    exist = true;
                    break;
                }
            }

            if (exist) return;


            this._logger.debug(correlationId, "Table " + this._tableName + " does not exist. Creating database objects...");

            // Run all DML commands
            for (var dml : this._schemaStatements)
                stmt.execute(dml);

        } catch (SQLException ex) {
            this._logger.error(correlationId, ex, "Failed to autocreate database object");
            throw new RuntimeException(ex);
        }
    }

    /**
     * Generates a list of column names to use in SQL statements like: "column1,column2,column3"
     *
     * @param values an array with column values or a key-value map
     * @return generated list of column names
     */
    protected String generateColumns(Map<String, Object> values) {
        var valuesList = values.keySet();

        StringBuilder result = new StringBuilder();

        for (var value : valuesList) {
            if (!result.toString().equals("")) result.append(",");
            result.append(this.quoteIdentifier(value));
        }

        return result.toString();
    }

    /**
     * Generates a list of value parameters to use in SQL statements like: "$1,$2,$3"
     *
     * @param values an array with values or a key-value map
     * @return a generated list of value parameters
     */
    protected String generateParameters(Map<String, Object> values) {
        var valuesList = values.values();
        return this.generateParameters(valuesList);
    }

    protected String generateParameters(Iterable<?> values) {
        StringBuilder result = new StringBuilder();
        for (var value : values) {
            if (!result.toString().equals("")) result.append(",");
            result.append("'").append(value).append("'");
        }

        return result.toString();
    }

    /**
     * Generates a list of column sets to use in UPDATE statements like: column1=$1,column2=$2
     *
     * @param values a key-value map with columns and values
     * @return a generated list of column sets
     */
    protected String generateSetParameters(Map<String, Object> values) {
        StringBuilder result = new StringBuilder();
        for (var column : values.keySet()) {
            if (!result.toString().equals("")) result.append(",");
            result.append(this.quoteIdentifier(column)).append("=").append("'").append(values.get(column)).append("'");
        }

        return result.toString();
    }

    /**
     * Gets a page of data items retrieved by a given filter and sorted according to sort parameters.
     * <p>
     * This method shall be called by a public getPageByFilter method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object
     * @param paging        (optional) paging parameters
     * @param sort          (optional) sorting JSON object
     * @param select        (optional) projection JSON object
     * @return a requested data page.
     */
    protected DataPage<T> getPageByFilter(String correlationId, String filter, PagingParams paging,
                                          String sort, String select) {

        select = select != null ? select : "*";
        var query = "SELECT " + select + " FROM " + this.quotedTableName();

        // Adjust max item count based on configuration
        paging = paging != null ? paging : new PagingParams();
        var skip = paging.getSkip(-1);
        var take = paging.getTake(this._maxPageSize);
        var pagingEnabled = paging.hasTotal();

        if (filter != null && !filter.isEmpty())
            query += " WHERE " + filter;

        if (sort != null)
            query += " ORDER BY " + sort;

        query += " LIMIT " + take;

        if (skip >= 0)
            query += " OFFSET " + skip;

        List<T> items = new ArrayList<>();
        var resultObjects = new ArrayList<Map<String, Object>>();

        try {
            var statement = this._client.createStatement();
            var rs = statement.executeQuery(query);

            // fetch all objects
            while (rs.next()) {
                var mapOb = new HashMap<String, Object>();
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
                    mapOb.put(rs.getMetaData().getColumnName(columnIndex), rs.getObject(columnIndex));
                resultObjects.add(mapOb);
            }

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        resultObjects.forEach((item) -> items.add(convertToPublic(item)));

        if (!items.isEmpty())
            this._logger.trace(correlationId, "Retrieved %d from %s", items.size(), this._tableName);

        if (pagingEnabled) {
            query = "SELECT COUNT(*) AS count FROM " + this.quotedTableName();
            if (filter != null && !filter.equals(""))
                query += " WHERE " + filter;

            long count = 0;

            try {
                var statement = this._client.createStatement();
                var rs = statement.executeQuery(query);

                if (rs.next())
                    count = rs.getLong(1);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }

            return new DataPage<>(items, count);
        } else {
            return new DataPage<>(items);
        }
    }

    /**
     * Gets a number of data items retrieved by a given filter.
     * <p>
     * This method shall be called by a public getCountByFilter method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object
     * @return a number of objects that satifsy the filter.
     */
    protected long getCountByFilter(String correlationId, Object filter) {
        var query = "SELECT COUNT(*) AS count FROM " + this.quotedTableName();
        if (filter != null && filter != "")
            query += " WHERE " + filter;

        long count = 0;

        try {
            var statement = this._client.createStatement();
            var rs = statement.executeQuery(query);

            if (rs.next())
                count = rs.getLong(1);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        this._logger.trace(correlationId, "Counted %d items in %s", count, this._tableName);

        return count;
    }

    /**
     * Gets a list of data items retrieved by a given filter and sorted according to sort parameters.
     * <p>
     * This method shall be called by a public getListByFilter method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object
     * @param sort          (optional) sorting JSON object
     * @param select        (optional) projection JSON object
     * @return a list with requested objects.
     */
    protected List<T> getListByFilter(String correlationId, String filter, String sort, String select) {
        select = select != null ? select : "*";
        var query = "SELECT " + select + " FROM " + this.quotedTableName();

        if (filter != null)
            query += " WHERE " + filter;

        if (sort != null)
            query += " ORDER BY " + sort;

        List<T> items = new ArrayList<>();
        var resultObjects = new ArrayList<Map<String, Object>>();

        try {
            var statement = this._client.createStatement();
            var rs = statement.executeQuery(query);

            // fetch all objects
            while (rs.next()) {
                var mapOb = new HashMap<String, Object>();
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
                    mapOb.put(rs.getMetaData().getColumnName(columnIndex), rs.getObject(columnIndex));
                resultObjects.add(mapOb);
            }

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        resultObjects.forEach((item) -> items.add(convertToPublic(item)));

        if (!items.isEmpty())
            this._logger.trace(correlationId, "Retrieved %d from %s", items.size(), this._tableName);

        return items;
    }

    /**
     * Gets a random item from items that match to a given filter.
     * <p>
     * This method shall be called by a public getOneRandom method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object
     * @return a random item that satisfies the filter.
     */
    protected T getOneRandom(String correlationId, String filter) {
        var query = "SELECT COUNT(*) AS count FROM " + this.quotedTableName();

        if (filter != null)
            query += " WHERE " + filter;

        long count = 0;

        try {
            var statement = this._client.createStatement();
            var rs = statement.executeQuery(query);

            if (rs.next())
                count = rs.getLong(1);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        query = "SELECT * FROM " + this.quotedTableName();

        if (filter != null)
            query += " WHERE " + filter;

        var pos = RandomInteger.nextInteger(0, (int) count);
        query += " LIMIT 1" + " OFFSET " + pos;

        T item;
        var resultMap = new HashMap<String, Object>();

        try {
            var statement = this._client.createStatement();
            var rs = statement.executeQuery(query);

            // fetch all objects
            if (rs.next())
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
                    resultMap.put(rs.getMetaData().getColumnName(columnIndex), rs.getObject(columnIndex));

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        item = convertToPublic(resultMap);

        if (item == null)
            this._logger.trace(correlationId, "Random item wasn't found from %s", this._tableName);
        else
            this._logger.trace(correlationId, "Retrieved random item from %s", this._tableName);

        return item;
    }

    /**
     * Creates a data item.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param item          an item to be created.
     * @return a created item.
     */
    public T create(String correlationId, T item) {
        if (item == null)
            return null;

        var row = this.convertFromPublic(item);
        var columns = this.generateColumns(row);
        var params = this.generateParameters(row);

        var query = "INSERT INTO " + this.quotedTableName() + " (" + columns + ") VALUES (" + params + ")";
        //query += "; SELECT * FROM " + this.quotedTableName();

        try {
            var statement = this._client.createStatement();
            statement.execute(query);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        this._logger.trace(correlationId, "Created in %s with id = %s", this.quotedTableName(), convertFromPublic(item).getOrDefault("id", null));

        return item;
    }

    /**
     * Deletes data items that match to a given filter.
     * <p>
     * This method shall be called by a public deleteByFilter method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object.
     */
    public void deleteByFilter(String correlationId, String filter) {
        var query = "DELETE FROM " + this.quotedTableName();
        if (filter != null)
            query += " WHERE " + filter;

        long count = 0;

        try {
            var statement = this._client.createStatement();
            var rs = statement.executeQuery(query);

            if (rs.next())
                count = rs.getLong(1);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        this._logger.trace(correlationId, "Deleted %d items from %s", count, this._tableName);
    }
}
