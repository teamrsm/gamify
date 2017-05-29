package com.gamify;

import java.io.IOException;
import java.lang.reflect.Field;

import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by Said on 1/22/2017.
 */
public class JsonJumbalay {

    public JsonJumbalay()
    {

    }

    public String Serialize(Object object) throws IOException, IllegalAccessException {
        XContentBuilder builder = jsonBuilder()
            .startObject();

        for(Field field: object.getClass().getDeclaredFields()) {
            field.setAccessible(true);  // set modifier to public
            Object value = field.get(object);

            builder.field(field.getName(), value.toString());
        }

        builder.endObject();

        return builder.string();
    }

    public String BuildJson (String fieldName, String fieldValue) throws IOException {
        XContentBuilder builder = jsonBuilder()
            .startObject()
                .field(fieldName, fieldValue)
            .endObject();

        return builder.string();
    }
}
