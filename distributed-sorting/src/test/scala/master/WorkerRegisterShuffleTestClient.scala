import com.master.server.MasterServer._
import com.master.server.MasterServer.MasterServerGrpc

import io.grpc.ManagedChannelBuilder
import scala.concurrent.ExecutionContext
import com.master.server.MasterServer.{WorkerInfo, RegisterReply}

object WorkerRegisterShuffleTestClient extends App {
    implicit val ec: ExecutionContext = ExecutionContext.global

    val ipArg   = args(0)
    val portArg = args(1).toInt

    val masterHost = if (args.length >= 3) args(2) else "localhost"
    val masterPort = if (args.length >= 4) args(3).toInt else 50057

    println(s"[ShuffleWorker] Call Register(shuffle): ip=$ipArg, port=$portArg, master=$masterHost:$masterPort")

    val channel = ManagedChannelBuilder
        .forAddress(masterHost, masterPort)
        .usePlaintext()
        .build()

    val bloc = MasterServerGrpc.blockingStub(channel)

    try {
        // isShuffle = true → read-only 조회용
        val reply: RegisterReply =
        bloc.register(WorkerInfo(ip = ipArg, port = portArg, isShuffle = true))

        val verStr = reply.version.map(_.version).getOrElse(0)
        println(s"[ShuffleWorker] reply.version = $verStr")
        println(s"[ShuffleWorker] worker list:")
        reply.workerInfos.foreach { w =>
        println(s"  - ${w.ip}:${w.port}")
        }
    } catch {
        case e: Throwable =>
        println(s"[ShuffleWorker] 실패...: ${e.getMessage}")
    } finally {
        channel.shutdown()
    }
}