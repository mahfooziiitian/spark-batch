package com.mahfooz.spark.orc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OrcDataGenerator {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        //TypeDescription schema = TypeDescription.fromString("struct<name:string,age:bigint,city:string>");
        TypeDescription schema = TypeDescription.createStruct()
                .addField("name", TypeDescription.createString())
                .addField("age", TypeDescription.createLong())
                .addField("city", TypeDescription.createString());
        String dataHome = System.getenv("DATA_HOME");
        String inputPath = String.format("%s/FileData/Orc/person.orc", dataHome);

        Writer writer = OrcFile.createWriter(new Path(inputPath),
                OrcFile.writerOptions(conf)
                        .setSchema(schema));
        VectorizedRowBatch batch = schema.createRowBatch();
        BytesColumnVector x = (BytesColumnVector) batch.cols[0];
        LongColumnVector y = (LongColumnVector) batch.cols[1];
        BytesColumnVector z = (BytesColumnVector) batch.cols[2];
        for(int r=0; r < 10000; ++r) {
            int row = batch.size++;
            x.setVal(row, ("Person"+r).getBytes(StandardCharsets.UTF_8));
            y.vector[row]=25 + r;
            z.setVal(row, ("City"+r).getBytes(StandardCharsets.UTF_8));
            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }
}
