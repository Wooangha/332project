package worker

import java.nio.file.{Paths, Files}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import common.{Data, Datum, Key}
import worker.io.{DatumFileIterator, DatumFileWriter}

object  DataProcessor {
  val tempDirPrefix = "tmpSave/"

  def sampling(dataDirLs: List[String], sampleSize: Int): Future[Vector[Key]] = {
    val sampleSizePerFile = Math.ceil(sampleSize.toDouble / dataDirLs.length.toDouble).toInt

    val futures = for (dir <- dataDirLs)
      yield Future {
        Data.fromFile(dir).sampling(sampleSizePerFile)
      }

    Future.sequence(futures).map(_.toVector.flatten)
  }

  def sortAndPartitioning(
      dataDirLs: List[String],
      partition: Key => String): Future[Unit] = {

    val futures = for ((inputDir, numOfData) <- dataDirLs.zip(0 until dataDirLs.length))
      yield Future {
        val data = Data.fromFile(inputDir).sort().partitioning(partition)
        for ((ip, partData) <- data) Future {
          partData.save(tempDirPrefix + ip + "-" + numOfData.toString)
        }
      }

    Future.sequence(futures).map(_ => ())
  }

  def removeTempDir(): Unit = {
    val tempDirPath = Paths.get(tempDirPrefix)
    if (Files.exists(tempDirPath)) {
      Files.walk(tempDirPath)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(path => Files.delete(path))
    }
  }

  def merge(dataDirLs: List[String], makeNewDir: () => String, maxSize: Int): Unit = {
    val dataIters = dataDirLs.map(dir => new DatumFileIterator(dir))

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
          new DatumFileWriter(makeNewDir(), savingData.toSeq).write()
          savingData.clear()
          size = 0
        }
      }

      if (savingData.nonEmpty) {
        new DatumFileWriter(makeNewDir(), savingData.toList).write()
        savingData.clear()
      }

    } finally {
      dataIters.foreach(_.close())
    }
  }
}
