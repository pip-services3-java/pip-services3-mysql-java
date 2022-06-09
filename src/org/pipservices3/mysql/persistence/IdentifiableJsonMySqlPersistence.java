package org.pipservices3.mysql.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.commons.data.AnyValueMap;
import org.pipservices3.commons.data.IIdentifiable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

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
     * @param value     an object in internal format to convert.
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
