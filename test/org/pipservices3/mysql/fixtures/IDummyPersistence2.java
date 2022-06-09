package org.pipservices3.mysql.fixtures;

import org.pipservices3.commons.data.AnyValueMap;
import org.pipservices3.commons.data.DataPage;
import org.pipservices3.commons.data.FilterParams;
import org.pipservices3.commons.data.PagingParams;
import org.pipservices3.data.IGetter;
import org.pipservices3.data.IPartialUpdater;
import org.pipservices3.data.IWriter;

import java.util.List;

public interface IDummyPersistence2 extends IGetter<Dummy2, Long>, IWriter<Dummy2, Long>, IPartialUpdater<Dummy2, Long> {
    DataPage<Dummy2> getPageByFilter(String correlationId, FilterParams filter, PagingParams paging);
    long getCountByFilter(String correlationId, FilterParams filter);
    List<Dummy2> getListByIds(String correlationId, List<Long> ids);

    Dummy2 getOneRandom(String correlationId, FilterParams filter);

    List<Dummy2> getListByFilter(String correlationId, FilterParams filter);
    Dummy2 getOneById(String correlationId, Long id);
    Dummy2 create(String correlationId, Dummy2 item);
    Dummy2 update(String correlationId, Dummy2 item);
    Dummy2 set(String correlationId, Dummy2 item);
    Dummy2 updatePartially(String correlationId, Long id, AnyValueMap data);
    Dummy2 deleteById(String correlationId, Long id);
    void deleteByIds(String correlationId, List<Long> id);
}
