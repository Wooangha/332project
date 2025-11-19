import com.master.server.MasterServer.MasterServerGrpc.MasterServer
import com.master.server.MasterServer.{WorkerInfo, RegisterReply, Version, SampleKeyData, PartitionRanges, CanShutdownWorkerServerReply}

import scala.concurrent.{ExecutionContext, Future, Promise}

import scala.collection.concurrent.TrieMap // for TrieMap in workerInfosMap

import java.util.concurrent.atomic.AtomicInteger // for Atomic Int in version

import scala.collection.mutable.ListBuffer // for watingRequestsForRegister

import com.google.protobuf.ByteString
import common.Key

class MasterServerImpl extends MasterServer {
    val NUM_OF_WORKERS = 3

    ////// for register ////////
    // thread-safe하게 TrieMap으로 구현
    private val workerInfosMap: TrieMap[String, Int] = TrieMap.empty

    // version 초기 값: 0, worker들이 write을 하지 않으니, 크게 cose 상관 없을 듯하여 atomic 변수로 선언
    private val currentVersion = new AtomicInteger(0)

    private val waitingRequestsForRegister: ListBuffer[Promise[RegisterReply]] = ListBuffer.empty


    ////// for getPartitionRange ///////
    private val sampleKeyBatches: ListBuffer[Vector[Key]] = ListBuffer.empty

    private val waitingRequestsForPartitionRange: ListBuffer[Promise[PartitionRanges]] = ListBuffer.empty
    
    //공용 lock 객체
    private val lock = new Object

    def register(request: WorkerInfo): Future[RegisterReply] = {
        val ip = request.ip
        val port = request.port

        // lock 밖에서 사용될 애들
        var promiseOpt: Option[Promise[RegisterReply]] = None
        var replyOpt: Option[RegisterReply] = None // lock 안에서는 콜백 못하니까 reply해야 되는 지 상태만 lock 안에서 결정하는 용도
        var drains: List[Promise[RegisterReply]] = Nil

        lock.synchronized{
        
            workerInfosMap += (ip -> port)

            val newVersion = currentVersion.incrementAndGet()

            def makeReply(replyVersion: Int): RegisterReply = {
                val snap = workerInfosMap.readOnlySnapshot()
                val workerList = snap.iterator.toSeq.map{ case (i,p) => WorkerInfo(ip = i, port = p) }

                RegisterReply(workerInfos = workerList, version = Some(Version(version = replyVersion)))
            }

            if(workerInfosMap.size < NUM_OF_WORKERS){
                val p = Promise[RegisterReply]()
                waitingRequestsForRegister += p
                promiseOpt = Some(p)
            }
            else{
                val reply = makeReply(newVersion)
                replyOpt = Some(reply)
                
                drains = waitingRequestsForRegister.toList
                waitingRequestsForRegister.clear()
            }
        }

        promiseOpt match{
            case Some(p) => p.future // 어차피 웨리포 닫는 워커가 미리 trySuccess로 complete해도 future니까 그대로 reply 문제 없음
            case None => {
                val reply = replyOpt.get // 논리상 None 불가능
                // 혹시나 waiting에 대기 중이던 워커가 죽거나 하는 등 fail 처리날 수도 있음(혹은 추후 타임 아웃 기능으로 취소 될 수도) -> 따라서 trySuccess
                drains.foreach(_.trySuccess(reply))
                Future.successful(reply)
            }
        }
    }

    def getPartitionRange(request: SampleKeyData): Future[PartitionRanges] = {
        var promiseOpt: Option[Promise[PartitionRanges]] = None
        var replyOpt: Option[PartitionRanges] = None
        var drains: List[Promise[PartitionRanges]] = Nil

        lock.synchronized{
            val keyBatch: Vector[Key] = request.keyData.map(k => Key(k.keyDatum.toByteArray().toVector)).toVector

            sampleKeyBatches += keyBatch

            if (sampleKeyBatches.size < NUM_OF_WORKERS) {
                val p = Promise[PartitionRanges]()
                waitingRequestsForPartitionRange += p
                promiseOpt = Some(p)
            }
            else {
                val allKeys: Vector[Key] = sampleKeyBatches.flatten.toVector

                val ranges: Seq[PartitionRanges.PartitionRange] =
                computePartitionRanges(allKeys, NUM_OF_WORKERS)

                val reply = PartitionRanges(partitionRanges = ranges)
                replyOpt = Some(reply)

                drains = waitingRequestsForPartitionRange.toList
                waitingRequestsForPartitionRange.clear()
            }
        }

        promiseOpt match {
            case Some(p) => p.future
            case None => {
                val reply = replyOpt.get
                drains.foreach(_.trySuccess(reply))
                Future.successful(reply)
            }
        }
    }

    private def computePartitionRanges(allKeys: Vector[Key], numPartitions: Int): Seq[PartitionRanges.PartitionRange] = {
        val sorted = allKeys.sorted
        val total  = sorted.length

        val ranges: ListBuffer[PartitionRanges.PartitionRange] = ListBuffer.empty

        for (i <- 0 until numPartitions) {

            val startIdx = (i * total) / numPartitions
            val endIdx   = ((i + 1) * total) / numPartitions

            val startKey =
            if (i == 0) Key.min
            else sorted(startIdx)

            val endKey =
            if (i == numPartitions - 1) Key.max
            else sorted(endIdx)

            val pr = PartitionRanges.PartitionRange(
                // 프로토 mutable 사용 안 되므로, copyFrom으로 Vector[Byte] -> Array[Byte] -> ByteString 변환
                startKey = ByteString.copyFrom(startKey.key.toArray),
                endKey   = ByteString.copyFrom(endKey.key.toArray)
            )

            ranges += pr
        }

        ranges.toSeq
    }

    def getUpdatedWorkerInfo(request: Version): Future[WorkerInfo] = ???

    def canShutdownWorkerServer(request: com.google.protobuf.empty.Empty): Future[CanShutdownWorkerServerReply] = ???

}