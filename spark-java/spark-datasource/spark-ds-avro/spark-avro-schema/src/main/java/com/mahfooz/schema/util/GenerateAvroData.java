package com.mahfooz.schema.util;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class GenerateAvroData {
    public static void main(String[] args) {
        // Create an Avro schema instance
        Schema schema = new Schema.Parser().parse("{ \"type\": \"record\", \"name\": \"User\", \"fields\": [ {\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"age\", \"type\": \"long\"} ]}");

        // Generate some sample data
        User user1 = new User(1, "Alice", 25);
        User user2 = new User(2, "Bob", 30);
        User user3 = new User(3, "Charlie", 22);

        String dataHome = System.getenv("DATA_HOME");
       String outputPath = String.format("%s/FileData/Avro/schema/user.avro",dataHome);
        // Serialize the data to Avro binary format
        writeAvroData(schema, outputPath, user1, user2, user3);
    }

    private static void writeAvroData(Schema schema, String outputPath, User... users) {
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        try {
            dataFileWriter.create(schema, new File(outputPath));

            for (User user : users) {
                GenericRecord record = user.toGenericRecord(schema);
                dataFileWriter.append(record);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dataFileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}