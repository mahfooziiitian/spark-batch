package com.mahfooz.schema.util

import java.nio.file.FileSystems
import java.nio.file.Paths

object  WindowsToUnixPathConverter {
  def convertToUnixPath(windowsPath: String): String = {
    val windowsSeparator = '\\'
    val unixSeparator = '/'

    val customFileSystem = FileSystems.newFileSystem(Paths.get(windowsPath), null)
    val unixPath = customFileSystem.getPath(windowsPath.replace(windowsSeparator, unixSeparator))
    val result = unixPath.toString

    customFileSystem.close()
    result
  }
  
   def main(args: Array[String]): Unit = {
    val windowsPath = "C:\\path\\to\\file.txt"
    val unixPath = convertToUnixPath(windowsPath)

    println(s"Windows Path: $windowsPath")
    println(s"Unix Path: $unixPath")
  }
}
