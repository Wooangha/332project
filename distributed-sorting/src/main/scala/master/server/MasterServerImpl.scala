import com.master.server.MasterServer.MasterServerGrpc.MasterServer
import com.master.server.MasterServer.{WorkerInfo, RegisterReply, Version}

import scala.concurrent.{ExecutionContext, Future, Promise}

import scala.collection.concurrent.TrieMap // for TrieMap in workerInfosMap

import java.util.concurrent.atomic.AtomicInteger // for Atomic Int in version

import scala.collection.mutable.ListBuffer // for watingRequestsForRegister


class MasterServerImpl extends MasterServer {
    val NUM_OF_WORKERS = 2

    // thread-safe하게 TrieMap으로 구현
    private val workerInfosMap: TrieMap[String, Int] = TrieMap.empty

    // version 초기 값: 0, worker들이 write을 하지 않으니, 크게 cose 상관 없을 듯하여 atomic 변수로 선언
    private val currentVersion = new AtomicInteger(0)

    private val waitingRequestsForRegister: ListBuffer[Promise[RegisterReply]] = ListBuffer.empty

    def register(request: WorkerInfo): Future[RegisterReply] = {
        val ip = request.ip
        val port = request.port
        
        // lock.synchronized 아직 미구현, 추후 lock.synchronized로 묶는다면 map 업데이트 부터 버전 비교 까지를 묶어야 할 듯?
        val prevOpt = workerInfosMap.put(ip, port)

        val isNewWorker = prevOpt.isEmpty // 전에 등록된 적 없던 ip일 경우에만 true

        val newVersion = if(isNewWorker) currentVersion.incrementAndGet() else currentVersion.get()

        def makeReply(replyVersion: Int): RegisterReply = {
            val snap = workerInfosMap.readOnlySnapshot()
            val workerList = snap.iterator.toSeq.map{ case (i,p) => WorkerInfo(ip = i, port = p) }

            RegisterReply(workerInfos = workerList, version = Some(Version(version = replyVersion)))
        }

        if(newVersion < NUM_OF_WORKERS){
            val p = Promise[RegisterReply]()
            waitingRequestsForRegister.synchronized{
                waitingRequestsForRegister += p
            }
            p.future
        }
        else{
            val reply = makeReply(newVersion)

            val drains: List[Promise[RegisterReply]] = waitingRequestsForRegister.synchronized{
                val copyOfWaitingRequests = waitingRequestsForRegister.toList
                waitingRequestsForRegister.clear()
                copyOfWaitingRequests
            }

            // 혹시나 waiting에 대기 중이던 워커가 죽거나 하는 등 fail 처리날 수도 있음(혹은 추후 타임 아웃 기능으로 취소 될 수도) -> 따라서 trySuccess
            drains.foreach(_.trySuccess(reply))
            Future.successful(reply)
        }
    }
    def canShutdownWorkerServer(request: com.google.protobuf.empty.Empty): scala.concurrent.Future[com.master.server.MasterServer.CanShutdownWorkerServerReply] = ???
    def getUpdatedWorkerInfo(request: com.master.server.MasterServer.Version): scala.concurrent.Future[com.master.server.MasterServer.WorkerInfo] = ???
    def getPartitionRange(request: com.master.server.MasterServer.SampleKeyData): scala.concurrent.Future[com.master.server.MasterServer.PartitionRanges] = ???

}