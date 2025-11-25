import com.master.server.MasterServer.MasterServerGrpc.MasterServer
import com.master.server.MasterServer.{WorkerInfo, RegisterRequest,RegisterReply, Version, SampleKeyData, PartitionRanges, Ip,CanShutdownWorkerServerReply}

import scala.concurrent.{ExecutionContext, Future, Promise}

import scala.collection.concurrent.TrieMap // for TrieMap in workerInfosMap

import java.util.concurrent.atomic.AtomicInteger // for Atomic Int in version

import scala.collection.mutable.ListBuffer // for watingRequestsForRegister

import com.google.protobuf.ByteString
import common.Key
import com.master.server.MasterServer.RegisterRequest

// below for canShutdownWorkerServer
import io.grpc.ManagedChannelBuilder
import scala.collection.mutable.{Set => MutableSet}
import scala.util.{Success, Failure}
import com.worker.server.WorkerServer.WorkerSeverGrpc
import com.google.protobuf.empty.Empty

class MasterServerImpl extends MasterServer {
    val NUM_OF_WORKERS = 3

    ////// for register ///////
    // thread-safe하게 TrieMap으로 구현
    private val workerInfosMap: TrieMap[String, Int] = TrieMap.empty

    // version 초기 값: 0, worker들이 write을 하지 않으니, 크게 cose 상관 없을 듯하여 atomic 변수로 선언
    private val currentVersion = new AtomicInteger(0)

    private val waitingRequestsForRegister: ListBuffer[Promise[RegisterReply]] = ListBuffer.empty


    ////// for getPartitionRange ///////
    // ip -> 그 워커가 보낸 샘플 키 벡터
    private val sampleKeyMap: TrieMap[String, Vector[Key]] = TrieMap.empty

    private val waitingRequestsForPartitionRange: ListBuffer[Promise[PartitionRanges]] = ListBuffer.empty
    

    ////// for canShutdownWorkerServer ///////
    private val shutdownRequestIps: MutableSet[String] = MutableSet.empty
    private var globalCanshutdownPromise: Option[Promise[CanShutdownWorkerServerReply]] = None

    //공용 lock 객체
    private val lock = new Object

    def register(request: RegisterRequest): Future[RegisterReply] = {
        val info = request.workerInfo.getOrElse{ throw new IllegalArgumentException("RegisterRequest.workerInfo is missing") }
        val ip = info.ip
        val port = info.port
        val isShuffle = request.isShuffle

        // lock 밖에서 사용될 애들
        var promiseOpt: Option[Promise[RegisterReply]] = None
        var replyOpt: Option[RegisterReply] = None // lock 안에서는 콜백 못하니까 reply해야 되는 지 상태만 lock 안에서 결정하는 용도
        var drains: List[Promise[RegisterReply]] = Nil

        lock.synchronized{

            def makeReply(replyVersion: Int): RegisterReply = {
                val snap = workerInfosMap.readOnlySnapshot()
                val workerList = snap.iterator.toSeq.map{ case (i,p) => WorkerInfo(ip = i, port = p) }

                RegisterReply(workerInfos = workerList, version = Some(Version(version = replyVersion)))
            }

            if(!isShuffle){ // 처음 등록 용 register
                workerInfosMap += (ip -> port)

                val newVersion = currentVersion.incrementAndGet()

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
            else{ // 셔플 단계에서 새로운 workerInfo 받기 위한, read-only register
                val curVersion = currentVersion.get()
                val reply = makeReply(curVersion)
                replyOpt = Some(reply)
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
            val ip = request.ip

            val keyBatch: Vector[Key] = request.keyData.map(k => Key(k.keyDatum.toByteArray())).toVector

            sampleKeyMap.update(ip, keyBatch)

            if (sampleKeyMap.size < NUM_OF_WORKERS) {
                val p = Promise[PartitionRanges]()
                waitingRequestsForPartitionRange += p
                promiseOpt = Some(p)
            }
            else { // 웨리포 완료 후 죽었다가 살아난 워커는 다시 compute 호출하므로 비효율적임 -> 나중에 optimization할 때가 오면 수정
                val allKeys: Vector[Key] = sampleKeyMap.values.flatten.toVector

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
                startKey = ByteString.copyFrom(startKey.key),
                endKey   = ByteString.copyFrom(endKey.key)
            )

            ranges += pr
        }

        ranges.toSeq
    }

    // def getUpdatedWorkerInfo(request: Version): Future[WorkerInfo] = ???

    def canShutdownWorkerServer(request: Ip): Future[CanShutdownWorkerServerReply] = {
        val callerIp = request.ip

        var needIsAliveCheck = false
        var workerSnapshot: Seq[(String, Int)] = Nil // workerInfosMap read-only로 읽으려고
        var promise: Promise[CanShutdownWorkerServerReply] = null

        lock.synchronized{
            // 이번 canShutdown 시도에서 사용될 promise 준비
            if(globalCanshutdownPromise.isEmpty){
                globalCanshutdownPromise = Some(Promise[CanShutdownWorkerServerReply]())
            }
            promise = globalCanshutdownPromise.get

            shutdownRequestIps += callerIp

            if(shutdownRequestIps.size == NUM_OF_WORKERS){
                workerSnapshot = workerInfosMap.readOnlySnapshot().iterator.toSeq
                needIsAliveCheck = true
            }
        }

        implicit val ec: ExecutionContext = ExecutionContext.global

        if(needIsAliveCheck){
            val checksF: Future[Seq[Boolean]] = Future.traverse(workerSnapshot) {
                case (ip, port) => {
                    Future{
                        val channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build()
                        val stub = WorkerSeverGrpc.blockingStub(channel)

                        try{
                            val reply = stub.isAlive(Empty.defaultInstance)
                            reply.isAlive && reply.isDone
                        } catch{ // 해당 ip, port로의 stub이 에러 나면(워커 서버 죽었다던지..) -> false
                            case _ : Throwable => false
                        } finally{
                            channel.shutdown()
                        }
                    }
                }
            }
            checksF.onComplete{
                case Success(results) => {
                    val allAlive = results.forall(_ == true)
                    val reply = CanShutdownWorkerServerReply(canShutdownWorkerServer = allAlive)
                    promise.trySuccess(reply)

                    lock.synchronized{
                        shutdownRequestIps.clear()
                        globalCanshutdownPromise = None
                    }
                }
                case Failure(_) => { // 그냥 기타 아무런 에러 나면, 그 하나의 stub 때문에 전체 canShutdown이 막힐 수 있어서 Failure 달아둠..
                    val reply = CanShutdownWorkerServerReply(canShutdownWorkerServer = false)
                    promise.trySuccess(reply)

                    lock.synchronized{
                        shutdownRequestIps.clear()
                        globalCanshutdownPromise = None
                    }
                }
            }
        }

        promise.future
    }

}