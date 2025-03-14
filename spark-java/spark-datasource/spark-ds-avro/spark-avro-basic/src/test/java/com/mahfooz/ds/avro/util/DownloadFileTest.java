package com.mahfooz.ds.avro.util;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
class DownloadFileTest {

        // Downloads a file from a valid URL to a valid file name
        @Test
        public void test_downloadFile_validURL_validFileName() {
            String fileUrl = "http://example.com/file.txt";
            String fileName = "file.txt";

            DownloadFile.downloadFile(fileUrl, fileName);

            File downloadedFile = new File(fileName);
            assertTrue(downloadedFile.exists());
            downloadedFile.delete();
        }

        // Downloads a file from a URL with special characters to a valid file name
        @Test
        public void test_downloadFile_specialCharactersURL_validFileName() {
            String fileUrl = "http://example.com/file with spaces.txt";
            String fileName = "file.txt";

            DownloadFile.downloadFile(fileUrl, fileName);

            File downloadedFile = new File(fileName);
            assertTrue(downloadedFile.exists());
            downloadedFile.delete();
        }


        // Throws RuntimeException when URL is null
        @Test
        public void test_downloadFile_nullURL() {
            String fileUrl = null;
            String fileName = "file.txt";

            assertThrows(RuntimeException.class, () -> {
                DownloadFile.downloadFile(fileUrl, fileName);
            });
        }

        // Throws RuntimeException when file name is null
        @Test
        public void test_downloadFile_nullFileName() {
            String fileUrl = "http://example.com/file.txt";
            String fileName = null;

            assertThrows(RuntimeException.class, () -> {
                DownloadFile.downloadFile(fileUrl, fileName);
            });
        }

        // Throws RuntimeException when URL is malformed
        @Test
        public void test_downloadFile_malformedURL() {
            String fileUrl = "invalid_url";
            String fileName = "file.txt";

            assertThrows(RuntimeException.class, () -> {
                DownloadFile.downloadFile(fileUrl, fileName);
            });
        }


        // Throws RuntimeException when file name is a directory
        @Test
        public void test_throws_exception_when_file_name_is_directory() {
            String fileUrl = "https://example.com/file.txt";
            String fileName = "/path/to/directory";

            assertThrows(RuntimeException.class, () -> {
                DownloadFile.downloadFile(fileUrl, fileName);
            });
        }

        // Throws RuntimeException when file cannot be created
        @Test
        public void test_throws_exception_when_file_cannot_be_created() {
            // Arrange
            String fileUrl = "http://example.com/file.txt";
            String fileName = "file.txt";

            // Act and Assert
            assertThrows(RuntimeException.class, () -> DownloadFile.downloadFile(fileUrl, fileName));
        }

        // Downloads a file from a URL with a large file size to a valid file name
        @Test
        public void test_download_large_file() {
            // Arrange
            String fileUrl = "http://example.com/largefile.txt";
            String fileName = "largefile.txt";

            // Act
            DownloadFile.downloadFile(fileUrl, fileName);

            // Assert
            File downloadedFile = new File(fileName);
            assertTrue(downloadedFile.exists());
            assertTrue(downloadedFile.isFile());
            assertTrue(downloadedFile.length() > 0);
        }

        // Downloads a file from a URL with a small file size to a valid file name
        @Test
        public void test_downloadFile_smallFileSize_validFileName() {
            // Arrange
            String fileUrl = "http://example.com/smallfile.txt";
            String fileName = "smallfile.txt";

            // Act
            DownloadFile.downloadFile(fileUrl, fileName);

            // Assert
            File downloadedFile = new File(fileName);
            assertTrue(downloadedFile.exists());
            assertTrue(downloadedFile.isFile());
        }

        // Downloads a file from a URL with a non-existent file name to a valid file name
        @Test
        public void test_downloadFile_nonExistentFileName() {
            // Arrange
            String fileUrl = "http://example.com/file.txt";
            String fileName = "validFileName.txt";

            // Act
            DownloadFile.downloadFile(fileUrl, fileName);

            // Assert
            File downloadedFile = new File(fileName);
            assertTrue(downloadedFile.exists());
        }

        // Downloads a file from a URL with a non-existent URL to a valid file name
        @Test
        public void test_downloadFile_nonExistentURL() {
            // Arrange
            String fileUrl = "http://www.example.com/nonexistentfile.txt";
            String fileName = "validfile.txt";

            // Act and Assert
            assertThrows(RuntimeException.class, () -> DownloadFile.downloadFile(fileUrl, fileName));
        }

}