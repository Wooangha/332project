package worker

import java.nio.file.{Paths, Files}

import scala.concurrent.Await

import org.scalatest.funsuite.AnyFunSuite

import common.Data
import common.{GenData, CheckSorted}
import worker.DataProcessor.sampling

class SamplingSuite extends AnyFunSuite with GenData with CheckSorted {

  test("Sample data from a file — samples exist in the data") {
    val inputDirs = List("src/test/resources/partition1", "src/test/resources/partition2", "src/test/resources/partition3")

    val partitionSize = 100000

    for ((inputDir, index) <- inputDirs.zip(0 until inputDirs.length))
      generateData(inputDir, index * partitionSize, partitionSize, skewed = false)

    val keys = Await.result(sampling(inputDirs, 10000), scala.concurrent.duration.Duration.Inf)

    val datas = inputDirs.map { dir => 
      Data.fromFile(dir).data.map(_.toTuple).toMap
    }

    keys.foreach { key =>
      assert {
        datas.exists { dataMap =>
          dataMap.contains(key)
        }
      }
    }

    cleanupGeneratedData()
  }
}