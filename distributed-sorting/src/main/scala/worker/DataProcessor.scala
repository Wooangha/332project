package worker

import java.nio.file.{Paths, Files}

import scala.collection.mutable
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

import common.{Data, Datum, Key}
import worker.io.{DatumFileIterator, DatumFileWriter}
import org.checkerframework.checker.units.qual.A

object DataProcessor {
  val tempDirPrefix = "tmpSave/"

  def sampling(
      dataDirLs: List[String],
      sampleSize: Int): Future[Array[Key]] = {
    val sampleSizePerFile = Math.ceil(sampleSize.toDouble / dataDirLs.length.toDouble).toInt

    val futures = for (dir <- dataDirLs)
      yield Future {
        Data.fromFile(dir).sampling(sampleSizePerFile)
      }

    Future.sequence(futures).map(_.toArray.flatten)
  }

  def sortAndPartitioning(
      dataDirLs: List[String],
      partition: Key => String): Future[List[String]] = {

    val dataLoadLs = for ((inputDir, numOfData) <- dataDirLs.zip(0 until dataDirLs.length))
      yield Future {
        (numOfData, Data.fromFile(inputDir).sort().partitioning(partition))
      }

    val savedDataLoadLs = dataLoadLs.map { dataLoad =>
      dataLoad.flatMap { case (numOfData, data) =>
        val saveFutures = for ((ip, partData) <- data) yield Future {
          val saveDir = s"${tempDirPrefix}${ip}-${numOfData.toString}"
          partData.save(saveDir)
          saveDir
        }
        Future.sequence(saveFutures)
      }
    }

    Future.sequence(savedDataLoadLs).flatMap { dirsLis =>
      Future.successful(dirsLis.flatten)
    }
  }

  def removeTempDir(): Unit = {
    val tempDirPath = Paths.get(tempDirPrefix)
    if (Files.exists(tempDirPath)) {
      Files.walk(tempDirPath, 1).forEach { path => 
        if (!Files.isDirectory(path)) {
          Files.delete(path)
        }
      }
    }
  }

  def merge(dataDirLs: List[String], makeNewDir: () => String, maxSize: Int): Unit = {
    val dataIters = dataDirLs.map(dir => new DatumFileIterator(dir))
    var saveFutures = List[Future[Unit]]()

    try {
      var size = 0
      val savingData = mutable.ListBuffer[Datum]()

      val ord = Ordering.by[(Datum, DatumFileIterator), Key](_._1.key).reverse
      val pq = mutable.PriorityQueue[(Datum, DatumFileIterator)]()(ord)

      dataIters.foreach { iter =>
        if (iter.hasNext) {
          val nextDatum = iter.next()
          pq.enqueue((nextDatum, iter))
        }
      }

      while (pq.nonEmpty) {
        val (minDatum, fromIter) = pq.dequeue()
        savingData += minDatum
        size += 1
        if (fromIter.hasNext) {
          val nextDatum = fromIter.next()
          pq.enqueue((nextDatum, fromIter))
        }
        if (size >= maxSize) {
          val saveDir = makeNewDir()
          println(s"Writing ${savingData.size} data to $saveDir...")
          val savingDataSeq = savingData.toSeq
          val saveFuture = Future {
            new DatumFileWriter(saveDir, savingDataSeq).write()
            println(s"Finished writing to $saveDir.")
          }
          saveFutures = saveFuture :: saveFutures
          savingData.clear()
          size = 0
        }
      }

      if (savingData.nonEmpty) {
        val saveDir = makeNewDir()
        val savingDataSeq = savingData.toSeq
        val saveFuture = Future {new DatumFileWriter(saveDir, savingDataSeq).write()}
        saveFutures = saveFuture :: saveFutures
        savingData.clear()
      }

      Await.result(
        Future.sequence(saveFutures),
        scala.concurrent.duration.Duration.Inf)

    } finally {
      dataIters.foreach(_.close())
    }
  }
}
