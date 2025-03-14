package com.mahfooz.sql.util
import java.net.URL
import java.nio.file.{Paths, Files, StandardCopyOption}

object DownloadFile {

  def fileDownload(url: String, destPath: String): Unit = {
    val urlObject = new URL(url)
    val destinationPath = Paths.get(destPath)

    try {
      Files.copy(
        urlObject.openStream(),
        destinationPath,
        StandardCopyOption.REPLACE_EXISTING
      )
      println(s"File downloaded to: $destinationPath")
    } catch {
      case e: Exception =>
        println(s"Error downloading the file: ${e.getMessage}")
    }
  }

}
