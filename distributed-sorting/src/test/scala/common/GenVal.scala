package common

import java.nio.file.{Paths, Files}

import scala.sys.process._
import scala.collection.mutable.Set

trait GenData {
  val gensortPath: String = Config.gensortPath

  private[this] val genDirectories: Set[String] = Set.empty

  final def generateData(
      dir: String,
      beginningRecord: Int,
      numRecords: Int,
      skewed: Boolean): Unit = {
    val emptyLogger = ProcessLogger(
      (o: String) => (),
      (e: String) => ()
    )
    if (skewed) {
      s"$gensortPath -s -b$beginningRecord $numRecords $dir".!(emptyLogger)
    } else {
      s"$gensortPath -b$beginningRecord $numRecords $dir".!(emptyLogger)
    }
    genDirectories += dir
  }

  final def cleanupGeneratedData(): Unit = {
    genDirectories.foreach { dir =>
      val path = Paths.get(dir)
      if (Files.exists(path)) {
        Files.delete(path)
      }
    }
    genDirectories.clear()
  }
}

trait CheckSorted {
  val valsortPath: String = Config.valsortPath
  
  private[this] val successRegex = "SUCCESS - all records are in order".r

  final def isSorted(dir: String): Boolean = {
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val logger = ProcessLogger(
      (o: String) => stdout.append(o).append('\n'),
      (e: String) => stderr.append(e).append('\n')
    )
    
    s"$valsortPath $dir".!(logger)
    
    successRegex.findFirstIn(stdout.toString).isDefined ||
    successRegex.findFirstIn(stderr.toString).isDefined
  }
}