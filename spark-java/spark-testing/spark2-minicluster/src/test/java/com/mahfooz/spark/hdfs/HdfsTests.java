package com.mahfooz.spark.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;

public class HdfsTests {

    private Configuration conf;
    private MiniDFSCluster cluster;
    private JavaSparkContext sparkContext;

    @BeforeAll
    public static void setupTests() throws Exception {
        // Force logging level for a job class
        LogManager.getLogger(HdfsTests.class).setLevel(Level.WARN);
    }

    @BeforeEach
    public void setup() throws Exception {
        File testDataPath = new File(getClass().getResource("/minicluster").getFile());
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        conf = new HdfsConfiguration();
        File testDataCluster1 = new File(testDataPath, "cluster1");
        String c1PathStr = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1PathStr);
        cluster = new MiniDFSCluster.Builder(conf).build();
        FileSystem.get(conf);

        SparkConf conf = new SparkConf().setAppName("LOG_ANALYZER").setMaster("local")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.io.compression.codec", "lz4");
        sparkContext = new JavaSparkContext(conf);
    }

    @AfterEach
    public void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
        if (sparkContext != null) {
            sparkContext.stop();
            sparkContext = null;
        }
    }
}