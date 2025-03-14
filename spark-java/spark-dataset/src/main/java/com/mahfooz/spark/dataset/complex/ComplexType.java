package com.mahfooz.spark.dataset.complex;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.mahfooz.spark.dataset.model.Line;
import com.mahfooz.spark.dataset.model.NamedPoints;
import com.mahfooz.spark.dataset.model.Point;
import com.mahfooz.spark.dataset.model.Segment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

//
// Examples of querying against more complex schema inferred from Java beans.
// Includes JavaBean nesting, arrays and maps.
//
public class ComplexType {
        public static void main(String[] args) {
                SparkSession spark = SparkSession.builder().appName("Dataset-ComplexType").master("local[4]")
                                .getOrCreate();

                //
                // Example 1: nested Java beans
                //

                System.out.println("*** Example 1: nested Java beans");

                Encoder<Segment> segmentEncoder = Encoders.bean(Segment.class);

                List<Segment> data = Arrays.asList(new Segment(new Point(1.0, 2.0), new Point(3.0, 4.0)),
                                new Segment(new Point(8.0, 2.0), new Point(3.0, 14.0)),
                                new Segment(new Point(11.0, 2.0), new Point(3.0, 24.0)));

                Dataset<Segment> ds = spark.createDataset(data, segmentEncoder);

                System.out.println("*** here is the schema inferred from the bean");
                ds.printSchema();

                System.out.println("*** here is the data");
                ds.show();

                // Use the convenient bean-inferred column names to query
                System.out.println("*** filter by one column and fetch others");
                ds.where(col("from").getField("x").gt(7.0)).select(col("to")).show();

                //
                // Example 2: arrays
                //

                System.out.println("*** Example 2: arrays");

                Encoder<Line> lineEncoder = Encoders.bean(Line.class);
                List<Line> lines = Arrays.asList(
                                new Line("a", new Point[] { new Point(0.0, 0.0), new Point(2.0, 4.0) }),
                                new Line("b", new Point[] { new Point(-1.0, 0.0) }), new Line("c", new Point[] {
                                                new Point(0.0, 0.0), new Point(2.0, 6.0), new Point(10.0, 100.0) }));

                Dataset<Line> linesDS = spark.createDataset(lines, lineEncoder);

                System.out.println("*** here is the schema inferred from the bean");
                linesDS.printSchema();

                System.out.println("*** here is the data");
                linesDS.show();

                // notice here you can filter by the second element of the array, which
                // doesn't even exist in one of the rows
                System.out.println("*** filter by an array element");
                linesDS.where(col("points").getItem(2).getField("y").gt(7.0))
                                .select(col("name"), size(col("points")).as("count")).show();

                //
                // Example 3: maps

                System.out.println("*** Example 3: maps");
                Encoder<NamedPoints> namedPointsEncoder = Encoders.bean(NamedPoints.class);
                HashMap<String, Point> points1 = new HashMap<>();
                points1.put("p1", new Point(0.0, 0.0));
                HashMap<String, Point> points2 = new HashMap<>();
                points2.put("p1", new Point(0.0, 0.0));
                points2.put("p2", new Point(2.0, 6.0));
                points2.put("p3", new Point(10.0, 100.0));
                List<NamedPoints> namedPoints = Arrays.asList(new NamedPoints("a", points1),
                                new NamedPoints("b", points2));

                Dataset<NamedPoints> namedPointsDS = spark.createDataset(namedPoints, namedPointsEncoder);

                System.out.println("*** here is the schema inferred from the bean");
                namedPointsDS.printSchema();

                System.out.println("*** here is the data");
                namedPointsDS.show();

                System.out.println("*** filter and select using map lookup");
                namedPointsDS.where(size(col("points")).gt(1))
                                .select(col("name"), size(col("points")).as("count"), col("points").getItem("p1"))
                                .show();

                spark.stop();
        }
}