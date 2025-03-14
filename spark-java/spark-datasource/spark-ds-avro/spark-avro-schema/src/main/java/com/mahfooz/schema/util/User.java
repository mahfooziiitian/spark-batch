package com.mahfooz.schema.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private int id;
    private String name;
    private long age;

    // Create a GenericRecord from the instance
    public GenericRecord toGenericRecord(Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", this.id);
        record.put("name", this.name);
        record.put("age", this.age);
        return record;
    }

    // Factory method to create a User instance from a GenericRecord
    public static User fromGenericRecord(GenericRecord record) {
        User user = new User();
        user.setId((Integer) record.get("id"));
        user.setName((String) record.get("name"));
        user.setAge((Long) record.get("age"));
        return user;
    }

}