package org.pipservices3.mysql.fixtures;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.pipservices3.commons.data.IStringIdentifiable;
import org.pipservices3.commons.data.IdGenerator;

public class Dummy implements IStringIdentifiable {

    public Dummy() {
    }

    public Dummy(String id, String key, String content) {
        super();
        this._id = id;
        this._key = key;
        this._content = content;
    }

    @JsonProperty("id")
    private String _id;

    public String getId() {
        return _id;
    }

    public void setId(String id) {
        this._id = id;
    }

    @Override
    public String withGeneratedId() {
        _id = IdGenerator.nextLong();
        return _id;
    }

    @JsonProperty("key")
    private String _key;

    public String getKey() {
        return _key;
    }

    public void setKey(String key) {
        this._key = key;
    }

    @JsonProperty("content")
    private String _content;

    public String getContent() {
        return _content;
    }

    public void setContent(String content) {
        this._content = content;
    }

}
