package org.pipservices3.mysql.fixtures;

import org.pipservices3.commons.data.AnyValueMap;
import org.pipservices3.commons.data.FilterParams;

import java.util.List;

import static org.junit.Assert.*;

public class DummyPersistenceFixture {
    private final Dummy _dummy1 = new Dummy(null, "key 1", "Content 1");
    private final Dummy _dummy2 = new Dummy(null, "key 2", "Content 2");

    private final IDummyPersistence _persistence;

    public DummyPersistenceFixture(IDummyPersistence persistence) {
        _persistence = persistence;
    }

    public void testCrudOperations() {
        // Create one dummy
        var dummy1 = this._persistence.create(null, this._dummy1);
        assertNotNull(dummy1);
        assertNotNull(dummy1.getId());
        assertEquals(this._dummy1.getKey(), dummy1.getKey());
        assertEquals(this._dummy1.getContent(), dummy1.getContent());

        // Create another dummy
        var dummy2 = this._persistence.create(null, this._dummy2);
        assertNotNull(dummy2);
        assertNotNull(dummy2.getId());
        assertEquals(this._dummy2.getKey(), dummy2.getKey());
        assertEquals(this._dummy2.getContent(), dummy2.getContent());

        // get all
        var page = this._persistence.getPageByFilter(null, null, null);
        assertNotNull(page);
        assertEquals(page.getData().size(), 2);

        // get with filters
        page = this._persistence.getPageByFilter(null, FilterParams.fromTuples("key", dummy2.getKey()), null);
        assertNotNull(page);
        assertEquals(page.getData().size(), 1);
        assertEquals(page.getData().get(0).getId(), dummy2.getId());
        assertEquals(page.getData().get(0).getKey(), dummy2.getKey());

        // get one random
        var item = _persistence.getOneRandom(null, null);
        assertNotNull(item);

        // Update the dummy
        dummy1.setContent("Updated Content 1");
        var result = this._persistence.update(null, dummy1);
        assertNotNull(result);
        assertEquals(dummy1.getId(), result.getId());
        assertEquals(dummy1.getKey(), result.getKey());
        assertEquals(dummy1.getContent(), result.getContent());

        // Set the dummy
        dummy1.setContent("Updated Content 2");
        result = this._persistence.set(null, dummy1);
        assertNotNull(result);
        assertEquals(dummy1.getId(), result.getId());
        assertEquals(dummy1.getKey(), result.getKey());
        assertEquals(dummy1.getContent(), result.getContent());

        // Partially update the dummy
        result = this._persistence.updatePartially(
                null, dummy1.getId(),
                AnyValueMap.fromTuples(
                        "content", "Partially Updated Content 1"
                )
        );
        assertNotNull(result);
        assertEquals(dummy1.getId(), result.getId());
        assertEquals(dummy1.getKey(), result.getKey());
        assertEquals("Partially Updated Content 1", result.getContent());

        // Get the dummy by Id
        result = this._persistence.getOneById(null, dummy1.getId());
        // Try to get item
        assertNotNull(result);
        assertEquals(dummy1.getId(), result.getId());
        assertEquals(dummy1.getKey(), result.getKey());
        assertEquals("Partially Updated Content 1", result.getContent());

        // Delete the dummy
        result = this._persistence.deleteById(null, dummy1.getId());
        assertNotNull(result);
        assertEquals(dummy1.getId(), result.getId());
        assertEquals(dummy1.getKey(), result.getKey());

        // Get the deleted dummy
        result = this._persistence.getOneById(null, dummy1.getId());
        // Try to get item
        assertNull(result);

        var count = this._persistence.getCountByFilter(null, null);
        assertEquals(count, 1);
    }

    public void testBatchOperations() {
        // Create one dummy
        var dummy1 = this._persistence.create(null, this._dummy1);
        assertNotNull(dummy1);
        assertNotNull(dummy1.getId());
        assertEquals(this._dummy1.getKey(), dummy1.getKey());
        assertEquals(this._dummy1.getContent(), dummy1.getContent());

        // Create another dummy
        var dummy2 = this._persistence.create(null, this._dummy2);
        assertNotNull(dummy2);
        assertNotNull(dummy2.getId());
        assertEquals(this._dummy2.getKey(), dummy2.getKey());
        assertEquals(this._dummy2.getContent(), dummy2.getContent());

        // Read batch
        var items = this._persistence.getListByIds(null, List.of(dummy1.getId(), dummy2.getId()));
        assertFalse(items.isEmpty());
        assertEquals(items.size(), 2);

        items = this._persistence.getListByIds(null, List.of(dummy1.getId()));
        assertNotNull(items);
        assertEquals(items.size(), 1);
        assertEquals(items.get(0).getId(), dummy1.getId());


        items = this._persistence.getListByFilter(null,FilterParams.fromTuples("key", dummy2.getKey()));
        assertFalse(items.isEmpty());
        assertEquals(items.size(), 1);
        assertEquals(items.get(0).getKey(), dummy2.getKey());

        // Delete batch
        this._persistence.deleteByIds(null, List.of(dummy1.getId(), dummy2.getId()));

        // Read empty batch
        items = this._persistence.getListByIds(null, List.of(dummy1.getId(), dummy2.getId()));
        assertTrue(items.isEmpty());
    }
}
