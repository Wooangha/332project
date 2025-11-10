package worker.io

import java.nio.file.{Paths, Files}

import scala.collection.mutable

object  DataProcessor {
  type Ip = String

  def partitioning(data: List[Datum], partition: Key => Ip): Map[Ip, List[Datum]] = {
    data.groupBy(datum => partition(datum.key))
  }

  def sort(data: List[Datum]): List[Datum] = data.sortBy(_.key)


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