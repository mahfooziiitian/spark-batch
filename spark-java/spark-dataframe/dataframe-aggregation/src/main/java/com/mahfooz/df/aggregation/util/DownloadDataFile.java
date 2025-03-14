package com.mahfooz.df.aggregation.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DownloadDataFile {

    public static void fileDownloader(String downloadFileUrl, String filePath){
        try(FileOutputStream fileInputStream= new FileOutputStream(filePath)){
            Files.copy(Paths.get(downloadFileUrl),fileInputStream);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
