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
      sampleSize: Int): Future[Array[Key]] = Future {
    var nowSampleSize = 0
    val dataDirIterator = dataDirLs.iterator

    var result = Array.empty[Key]

    while (nowSampleSize < sampleSize && dataDirIterator.hasNext) {
      val dir = dataDirIterator.next()
      val data = Data.fromFile(dir)
      
      val dataSize = data.data.length
      val takeSize = Math.min(sampleSize - nowSampleSize, dataSize)
      nowSampleSize += takeSize

      result ++= data.data.take(takeSize).map(_.key)
    }

    result
  }

  def sortAndPartitioning(
      dataDirLs: List[String],
      partition: Key => String): Future[List[String]] = Future {
    
    if (WorkerDashboard.verbose) {
      WorkerDashboard.WorkerManager.initSortPartitioning(dataDirLs.length)
    }
    val dataLoadLs = for ((inputDir, numOfData) <- dataDirLs.zip(0 until dataDirLs.length))
      yield {
        if (WorkerDashboard.verbose) {
          WorkerDashboard.WorkerManager.incSortPartitioning()
        }
        Data.fromFile(inputDir).sort().partitioning(partition).map { case (ip, partData) =>
          val saveDir = s"${tempDirPrefix}${ip}-${numOfData.toString}"
          partData.save(saveDir)
          saveDir
        }
      }

    dataLoadLs.flatten
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

      if (WorkerDashboard.verbose) {
        WorkerDashboard.WorkerManager.initMerging(1)
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
          val savingDataSeq = savingData.toSeq
          val saveFuture = Future {
            new DatumFileWriter(saveDir, savingDataSeq).write()

            if (WorkerDashboard.verbose) {
              WorkerDashboard.WorkerManager.incMerging()
            }
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
