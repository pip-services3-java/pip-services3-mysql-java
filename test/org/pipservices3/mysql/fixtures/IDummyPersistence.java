package org.pipservices3.mysql.fixtures;

import org.pipservices3.commons.data.AnyValueMap;
import org.pipservices3.commons.data.DataPage;
import org.pipservices3.commons.data.FilterParams;
import org.pipservices3.commons.data.PagingParams;
import org.pipservices3.data.IGetter;
import org.pipservices3.data.IPartialUpdater;
import org.pipservices3.data.IWriter;

import java.util.List;

public interface IDummyPersistence extends IGetter<Dummy, String>, IWriter<Dummy, String>, IPartialUpdater<Dummy, String> {
    DataPage<Dummy> getPageByFilter(String correlationId, FilterParams filter, PagingParams paging);
    long getCountByFilter(String correlationId, FilterParams filter);
    List<Dummy> getListByIds(String correlationId, List<String> ids);

    Dummy getOneRandom(String correlationId, FilterParams filter);

    List<Dummy> getListByFilter(String correlationId, FilterParams filter);
    Dummy getOneById(String correlationId, String id);
    Dummy create(String correlationId, Dummy item);
    Dummy update(String correlationId, Dummy item);
    Dummy set(String correlationId, Dummy item);
    Dummy updatePartially(String correlationId, String id, AnyValueMap data);
    Dummy deleteById(String correlationId, String id);
    void deleteByIds(String correlationId, List<String> id);
}
