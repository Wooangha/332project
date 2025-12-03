package worker

import java.security.KeyException

import common.Key

class PartitionNotFoundException(msg: String) extends KeyException(msg)

object Util {

  /**
    * Make a partition function from partition ranges and worker infos
    *
    * @param partitionRange
    * @param workerInfos
    * @return
    * @throws KeyException if no partition is found for a key
    */
  def makePartition(partitionRange: List[(Key, Key)], workerInfos: List[String]): Key => String = {
    val sortedRanges = partitionRange.sortBy(_._1)
    val sortedWorkerInfos = workerInfos.sorted

    key: Key => {
      if (key == Key.max) {
        sortedWorkerInfos.last
      } else {
        val idx = sortedRanges.indexWhere { case (start, end) =>
          key >= start && key < end
        }
        if (idx == -1) {
          throw new PartitionNotFoundException(
            s"No partition found for key: ${key}, now partitionRanges: ${partitionRange.toString()}")
        } else {
          sortedWorkerInfos(idx)
        }
      }
    }
  }

}