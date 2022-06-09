package org.pipservices3.mysql.persistence;

import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.commons.data.AnyValueMap;
import org.pipservices3.commons.data.IIdentifiable;
import org.pipservices3.data.IGetter;
import org.pipservices3.data.ISetter;
import org.pipservices3.data.IWriter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IdentifiableMySqlPersistence<T extends IIdentifiable<K>, K> extends MySqlPersistence<T>
        implements IWriter<T, K>, IGetter<T, K>, ISetter<T> {

    /**
     * Flag to turn on auto generation of object ids.
     */
    protected boolean _autoGenerateId = true;


    /**
     * Creates a new instance of the persistence component.
     *
     * @param tableName  (optional) a table name.
     * @param schemaName (optional) a schema name
     */
    public IdentifiableMySqlPersistence(Class<T> documentClass, String tableName, String schemaName) {
        super(documentClass, tableName, schemaName);
    }

    public IdentifiableMySqlPersistence(Class<T> documentClass) {
        super(documentClass);
    }

    /**
     * Converts the given object from the public partial format.
     *
     * @param value the object to convert from the public partial format.
     * @return the initial object.
     */
    protected Map<String, Object> convertFromPublicPartial(Object value) {
        return this.convertFromPublic(value);
    }

    /**
     * Gets a list of data items retrieved by given unique ids.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param ids           ids of data items to be retrieved
     * @return a list with requested data items.
     */
    public List<T> getListByIds(String correlationId, List<K> ids) {
        var params = this.generateParameters(ids);
        var query = "SELECT * FROM " + this.quotedTableName() + " WHERE id IN(" + params + ")";

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
     * Gets a data item by its unique id.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param id            an id of data item to be retrieved.
     * @return a requested data item or <code>null</code> if nothing was found.
     */
    @Override
    public T getOneById(String correlationId, K id) {
        var query = "SELECT * FROM " + this.quotedTableName() + " WHERE id=" + "'" + id + "'";

        T item;
        var resultMap = new HashMap<String, Object>();

        try {
            var stmt = this._client.createStatement();
            var rs = stmt.executeQuery(query);

            if (rs.next())
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
                    resultMap.put(rs.getMetaData().getColumnName(columnIndex), rs.getObject(columnIndex));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        item = this.convertToPublic(resultMap);

        if (item == null)
            this._logger.trace(correlationId, "Nothing found from %s with id = %s", this._tableName, id);
        else
            this._logger.trace(correlationId, "Retrieved from %s with id = %s", this._tableName, id);

        return item;
    }

    @Override
    public T create(String correlationId, T item) {
        if (item == null)
            return null;

        // Assign unique id
        var newItem = item;
        if (newItem.getId() == null && this._autoGenerateId) {

            try {
                // copy object
                newItem = JsonConverter.fromJson(_documentClass, JsonConverter.toJson(newItem));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            newItem.setId(newItem.withGeneratedId());
        }

        return super.create(correlationId, newItem);
    }

    /**
     * Sets a data item. If the data item exists it updates it,
     * otherwise it create a new data item.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param item          a item to be set.
     * @return the updated item.
     */
    @Override
    public T set(String correlationId, T item) {
        if (item == null)
            return null;

        // Assign unique id
        if (item.getId() == null && this._autoGenerateId) {
            // copy object
            try {
                item = JsonConverter.fromJson(_documentClass, JsonConverter.toJson(item));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            item.setId(item.withGeneratedId());
        }

        var row = this.convertFromPublic(item);
        var columns = this.generateColumns(row);
        var params = this.generateParameters(row);
        var setParams = this.generateSetParameters(row);

        var query = "INSERT INTO " + this.quotedTableName() + " (" + columns + ") VALUES (" + params + ")";
        query += " ON DUPLICATE KEY UPDATE " + setParams + ";";

        T newItem;
        var resultMap = new HashMap<String, Object>();

        try {
            var stmt = _client.createStatement();
            stmt.execute(query);

            query = "SELECT * FROM " + this.quotedTableName() + " WHERE id=" + "'" + item.getId().toString() + "'";

            var rs = stmt.executeQuery(query);

            // fetch results
            if (rs.next())
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
                    resultMap.put(rs.getMetaData().getColumnName(columnIndex), rs.getObject(columnIndex));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        this._logger.trace(correlationId, "Set in %s with id = %s", this.quotedTableName(), item.getId());

        newItem = this.convertToPublic(resultMap);
        return newItem;
    }

    @Override
    public T update(String correlationId, T item) {
        if (item == null || item.getId() == null)
            return null;

        var id = "'" + item.getId().toString() + "'";

        var row = this.convertFromPublic(item);
        var params = this.generateSetParameters(row);

        var query = "UPDATE " + this.quotedTableName() + " SET " + params + " WHERE id=" + id + ";";

        T newItem;
        var resultMap = new HashMap<String, Object>();

        try {
            var stmt = _client.createStatement();
            stmt.execute(query);

            query = "SELECT * FROM " + this.quotedTableName() + " WHERE id=" + id;

            var rs = stmt.executeQuery(query);
            // fetch all objects
            if (rs.next())
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
                    resultMap.put(rs.getMetaData().getColumnName(columnIndex), rs.getObject(columnIndex));

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        this._logger.trace(correlationId, "Updated in %s with id = %s", this._tableName, item.getId());

        newItem = this.convertToPublic(resultMap);
        return newItem;
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

        String strId = "'" + id.toString() + "'";
        var row = this.convertFromPublicPartial(data.getAsObject());
        var params = this.generateSetParameters(row);

        var query = "UPDATE " + this.quotedTableName() + " SET " + params + " WHERE id=" + strId + ";";


        T newItem;
        var resultMap = new HashMap<String, Object>();

        try {
            var stmt = _client.createStatement();
            stmt.execute(query);

            query = "SELECT * FROM " + this.quotedTableName() + " WHERE id=" + strId + ";";
            var rs = stmt.executeQuery(query);

            // fetch all objects
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

    @Override
    public T deleteById(String correlationId, K id) {
        var strId = "'" + id.toString() + "'";

        var query = "SELECT * FROM " + this.quotedTableName() + " WHERE id=" + strId;
        query += "; DELETE FROM " + this.quotedTableName() + " WHERE id=" + strId;

        T item;
        var resultMap = new HashMap<String, Object>();

        try {
            var stmt = _client.createStatement();
            var rs = stmt.executeQuery(query);

            // fetch all objects
            if (rs.next())
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
                    resultMap.put(rs.getMetaData().getColumnName(columnIndex), rs.getObject(columnIndex));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        this._logger.trace(correlationId, "Deleted from %s with id = %s", this._tableName, id);

        item = this.convertToPublic(resultMap);
        return item;
    }

    /**
     * Deletes multiple data items by their unique ids.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param ids           ids of data items to be deleted.
     */
    public void deleteByIds(String correlationId, List<K> ids) {
        var params = this.generateParameters(ids);
        var query = "DELETE FROM " + this.quotedTableName() + " WHERE id IN(" + params + ")";

        var count = 0;

        try {
            var stmt = _client.createStatement();
            count = stmt.executeUpdate(query);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        this._logger.trace(correlationId, "Deleted %d items from %s", count, this._tableName);
    }
}
