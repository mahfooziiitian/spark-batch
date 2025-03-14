package com.mahfooz.spark.dataframe.util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class DownloadFile {
    public static void downloadFile(String fileUrl, String fileName) {
        try {
                URL url = new URL(fileUrl);
                FileUtils.copyURLToFile(url, new File(fileName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
    }
}
