import com.master.server.MasterServer._
import com.master.server.MasterServer.MasterServerGrpc

import io.grpc.ManagedChannelBuilder
import scala.concurrent.ExecutionContext
import com.master.server.MasterServer.{WorkerInfo, RegisterRequest, RegisterReply}

object WorkerRegisterInitTestClient extends App {
    implicit val ec: ExecutionContext = ExecutionContext.global

    val ipArg   = args(0)
    val portArg = args(1).toInt

    val masterHost = if (args.length >= 3) args(2) else "localhost"
    val masterPort = if (args.length >= 4) args(3).toInt else 50057

    println(s"[InitWorker] Call Register(init): ip=$ipArg, port=$portArg, master=$masterHost:$masterPort")

    val channel = ManagedChannelBuilder
        .forAddress(masterHost, masterPort)
        .usePlaintext()
        .build()

    val bloc = MasterServerGrpc.blockingStub(channel)

    try {
        val req = RegisterRequest(
        workerInfo = Some(WorkerInfo(ip = ipArg, port = portArg)),
        isShuffle = false
        )

        val reply: RegisterReply = bloc.register(req)

        val verStr = reply.version.map(_.version).getOrElse(0)
        println(s"[InitWorker] reply.version = $verStr")
        reply.workerInfos.foreach { w => println(s"  - ${w.ip}:${w.port}") }

    } catch {
        case e: Throwable => println(s"[InitWorker] 실패...: ${e.getMessage}")
    } finally {
        channel.shutdown()
    }
}