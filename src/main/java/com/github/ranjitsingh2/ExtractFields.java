package com.github.ranjitsingh2;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
public abstract class ExtractFields <R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Extract multiple fields into a string";
    public static final String FIELDS_CONFIG = "fields";
    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, "Field names to extract");
    private static final String PURPOSE = "extracting fields into string";
    private List<String> fields;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = config.getList(FIELDS_CONFIG);
    }

    @Override
    public R apply(R record) {
//        if (record.valueSchema() == null) {
        return applySchemaless(record);
//        } else {
//            return applyWithSchema(record);
//        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        String dummyvalue = "";
        for (String field : fields) {
            dummyvalue=dummyvalue+value.get(field).toString();
        }
        return newRecord(record,null,dummyvalue);
    }

//    private R applyWithSchema(R record) {
//        final Struct value = requireStruct(record.value(), PURPOSE);
//        Schema keySchema = valueToKeySchemaCache.get(value.schema());
//    }
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);
}
