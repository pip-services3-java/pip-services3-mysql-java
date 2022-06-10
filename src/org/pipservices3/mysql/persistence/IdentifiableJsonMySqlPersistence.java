package org.pipservices3.mysql.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.commons.data.AnyValueMap;
import org.pipservices3.commons.data.IIdentifiable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract persistence component that stores data in MySQL in JSON or JSONB fields
 * and implements a number of CRUD operations over data items with unique ids.
 * The data items must implement {@link IIdentifiable} interface.
 * <p>
 * The JSON table has only two fields: id and data.
 * <p>
 * In basic scenarios child classes shall only override [[getPageByFilter]],
 * {@link #getListByFilter} or {@link #deleteByFilter} operations with specific filter function.
 * All other operations can be used out of the box.
 * <p>
 * In complex scenarios child classes can implement additional operations by
 * accessing <code>this._collection</code> and <code>this._model</code> properties.
 * <p>
 * ### Configuration parameters ###
 *
 * <pre>
 * - table:                  (optional) MySQL table name
 * - schema:                 (optional) MySQL schema name
 * - connection(s):
 *   - discovery_key:             (optional) a key to retrieve the connection from {@link org.pipservices3.components.connect.IDiscovery}
 *   - host:                      host name or IP address
 *   - port:                      port number (default: 3306)
 *   - uri:                       resource URI or connection string with all parameters in it
 * - credential(s):
 *   - store_key:                 (optional) a key to retrieve the credentials from {@link org.pipservices3.components.auth.ICredentialStore}
 *   - username:                  (optional) user name
 *   - password:                  (optional) user password
 * - options:
 *   - connect_timeout:      (optional) number of milliseconds to wait before timing out when connecting a new client (default: 0)
 *   - idle_timeout:         (optional) number of milliseconds a client must sit idle in the pool and not be checked out (default: 10000)
 *   - max_pool_size:        (optional) maximum number of clients the pool should contain (default: 10)
 *  </pre>
 * <p>
 * ### References ###
 * <p>
 * - *:logger:*:*:1.0          (optional) {@link org.pipservices3.components.log.ILogger} components to pass log messages components to pass log messages
 * - *:discovery:*:*:1.0        (optional) {@link org.pipservices3.components.connect.IDiscovery} services
 * - *:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials
 * <p>
 * ### Example ###
 * <pre>
 * {@code
 * public class MyJsonMySqlPersistence extends IdentifiableMySqlJsonPersistence<MyData, String> {
 *
 *     public MyJsonMySqlPersistence() {
 *         super(MyData.class, "dummies", null);
 *     }
 *
 *     @Override
 *     protected void defineSchema() {
 *         this.clearSchema();
 *         this.ensureTable();
 *         this.ensureSchema("ALTER TABLE `" + this._tableName + "` ADD `data_key` VARCHAR(50) AS (JSON_UNQUOTE(`data`->\"$.key\"))");
 *         this.ensureIndex(this._tableName + "_json_key", Map.of("data_key", 1), Map.of("unique", true));
 *     }
 *
 *     public DataPage<MyData> getPageByFilter(String correlationId, FilterParams filter, PagingParams paging) {
 *         filter = filter != null ? filter : new FilterParams();
 *         var key = filter.getAsNullableString("key");
 *
 *         String filterCondition = null;
 *         if (key != null)
 *             filterCondition = "data->'$.key'='" + key + "'";
 *
 *         return super.getPageByFilter(correlationId, filterCondition, paging, null, null);
 *     }
 * }
 * ...
 * var persistence = new MyJsonMySqlPersistence(MyData.class);
 * persistence.configure(ConfigParams.fromTuples(
 *         "host", "localhost",
 *         "port", 3306
 * ));
 *
 * persistence.open(null);
 *
 * persistence.create("123", new MyData("1", "ABC", "content"));
 * var page = persistence.getPageByFilter("123", FilterParams.fromTuples("key", "ABC"), null);
 *
 * var deletedItem = persistence.deleteById("123", "1");
 * }
 * </pre>
 */
public class IdentifiableJsonMySqlPersistence<T extends IIdentifiable<K>, K> extends IdentifiableMySqlPersistence<T, K> {

    /**
     * Creates a new instance of the persistence component.
     *
     * @param documentClass generic type of the class
     * @param tableName     (optional) a table name.
     * @param schemaName    (optional) a schema name.
     */
    public IdentifiableJsonMySqlPersistence(Class<T> documentClass, String tableName, String schemaName) {
        super(documentClass, tableName, schemaName);
    }

    public IdentifiableJsonMySqlPersistence(Class<T> documentClass) {
        super(documentClass, null, null);
    }

    /**
     * Adds DML statement to automatically create JSON(B) table
     *
     * @param idType   type of the id column (default: VARCHAR(32))
     * @param dataType type of the data column (default: JSON)
     */
    protected void ensureTable(String idType, String dataType) {
        if (idType == null)
            idType = "VARCHAR(32)";
        if (dataType == null)
            dataType = "JSON";

        if (this._schemaName != null) {
            var query = "CREATE SCHEMA IF NOT EXISTS " + this.quoteIdentifier(this._schemaName);
            this.ensureSchema(query);
        }
        var query = "CREATE TABLE IF NOT EXISTS " + this.quotedTableName()
                + " (`id` " + idType + " PRIMARY KEY, `data` " + dataType + ")";
        this.ensureSchema(query);
    }

    /**
     * Adds DML statement to automatically create JSON(B) table
     */
    protected void ensureTable() {
        this.ensureTable("VARCHAR(32)", "JSON");
    }

    /**
     * Converts object value from internal to public format.
     *
     * @param value an object in internal format to convert.
     * @return converted object in public format.
     */
    @Override
    protected T convertToPublic(Map<String, Object> value) {
        if (value == null) return null;
        try {
            return JsonConverter.fromJson(_documentClass, (String) value.getOrDefault("data", null));
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
    @Override
    protected Map<String, Object> convertFromPublic(Object value) {
        if (value == null) return null;

        try {
            return Map.of(
                    "id", ((IIdentifiable<K>) value).getId(),
                    "data", JsonConverter.toJson(value)
            );
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Updates only few selected fields in a data item.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param id            an id of data item to be updated.
     * @param data          a map with fields to be updated.
     * @return the updated item.
     */
    public T updatePartially(String correlationId, K id, AnyValueMap data) {
        if (data == null || id == null)
            return null;

        var strId = "'" + id.toString() + "'";

        String values;

        try {
            values = JsonConverter.toJson(data.getAsObject());
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }

        var query = "UPDATE " + this.quotedTableName() + " SET `data`=JSON_MERGE_PATCH(data,'" + values + "') WHERE id=" + strId;

        T newItem;
        var resultMap = new HashMap<String, Object>();

        try (var stmt = _client.createStatement()) {
            stmt.execute(query);
            query = "SELECT * FROM " + this.quotedTableName() + " WHERE id=" + strId;
            var rs = stmt.executeQuery(query);
            if (rs.next())
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
                    resultMap.put(rs.getMetaData().getColumnName(columnIndex), rs.getObject(columnIndex));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        this._logger.trace(correlationId, "Updated partially in %s with id = %s", this._tableName, id);

        newItem = this.convertToPublic(resultMap);
        return newItem;
    }

}
