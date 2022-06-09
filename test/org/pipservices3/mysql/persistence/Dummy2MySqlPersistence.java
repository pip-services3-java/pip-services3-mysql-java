package org.pipservices3.mysql.persistence;

import org.pipservices3.commons.data.DataPage;
import org.pipservices3.commons.data.FilterParams;
import org.pipservices3.commons.data.PagingParams;
import org.pipservices3.mysql.fixtures.Dummy2;
import org.pipservices3.mysql.fixtures.Dummy2;
import org.pipservices3.mysql.fixtures.IDummyPersistence;
import org.pipservices3.mysql.fixtures.IDummyPersistence2;

import java.util.List;
import java.util.Map;

public class Dummy2MySqlPersistence extends IdentifiableMySqlPersistence<Dummy2, Long>
        implements IDummyPersistence2 {

    public Dummy2MySqlPersistence() {
        super(Dummy2.class, "dummies2", null);
        this._autoGenerateId = false;
    }

    @Override
    protected void defineSchema() {
        this.clearSchema();
        this.ensureSchema("CREATE TABLE `" + this._tableName + "` (id VARCHAR(32) PRIMARY KEY, `key` VARCHAR(50), `content` TEXT)");
        this.ensureIndex(this._tableName + "_key", Map.of("key", 1), Map.of("unique", true));
    }

    @Override
    public DataPage<Dummy2> getPageByFilter(String correlationId, FilterParams filter, PagingParams paging) {
        filter = filter != null ? filter : new FilterParams();
        var key = filter.getAsNullableString("key");

        String filterCondition = null;
        if (key != null)
            filterCondition = "`key`='" + key + "'";

        return super.getPageByFilter(correlationId, filterCondition, paging, null, null);
    }

    @Override
    public long getCountByFilter(String correlationId, FilterParams filter) {
        filter = filter != null ? filter : new FilterParams();
        var key = filter.getAsNullableString("key");

        String filterCondition = null;
        if (key != null)
            filterCondition = "`key`='" + key + "'";

        return super.getCountByFilter(correlationId, filterCondition);
    }

    @Override
    public Dummy2 getOneRandom(String correlationId, FilterParams filter) {
        filter = filter != null ? filter : new FilterParams();
        var key = filter.getAsNullableString("key");

        String filterCondition = null;
        if (key != null)
            filterCondition = "`key`='" + key + "'";

        return super.getOneRandom(correlationId, filterCondition);
    }

    @Override
    public List<Dummy2> getListByFilter(String correlationId, FilterParams filter) {
        filter = filter != null ? filter : new FilterParams();
        var key = filter.getAsNullableString("key");

        String filterCondition = null;
        if (key != null)
            filterCondition = "`key`='" + key + "'";

        return super.getListByFilter(correlationId, filterCondition, null, null);
    }
}
