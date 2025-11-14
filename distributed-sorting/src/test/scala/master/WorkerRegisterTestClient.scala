import com.master.server.MasterServer._
import com.master.server.MasterServer.MasterServerGrpc

import io.grpc.ManagedChannelBuilder
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import com.master.server.MasterServer.MasterServerGrpc

object WorkerRegisterTestClient extends App {
    implicit val ec: ExecutionContext = ExecutionContext.global

    val ipArg = args(0)
    val portArg = args(1).toInt

    val channel = ManagedChannelBuilder
    .forAddress("localhost", 50057)
    .usePlaintext()
    .build()

    /*
    val stub = MasterServerGrpc.stub(channel)

    println(s"[Worker] Call Register: ip=$ipArg, port = $portArg")

    val f: Future[RegisterReply] = stub.register(WorkerInfo(ip = ipArg, port = portArg))

    f.onComplete{
        case Success(reply) => {
            println(s"[Worker] reply.version = ${reply.version.map(_.version)}")
            println(s"[Worker] worker list:")
            reply.workerInfos.foreach{
                w => println(s"  - ${w.ip}:${w.port}")
            }
            channel.shutdown()
        }
        case Failure(exception) => {
            println(s"[Worker] 실패...: ${exception.getMessage}")
            channel.shutdown()
        }
    }
    */

    val bloc = MasterServerGrpc.blockingStub(channel)

    println(s"[Worker] Call Register: ip=$ipArg, port=$portArg")

    try {
        // 동기 호출: 여기서 서버 응답이 올 때까지 현재 쓰레드가 블록됨
        val reply: RegisterReply =
        bloc.register(WorkerInfo(ip = ipArg, port = portArg))

        println(s"[Worker] reply.version = ${reply.version.map(_.version)}")
        println(s"[Worker] worker list:")
        reply.workerInfos.foreach { w =>
        println(s"  - ${w.ip}:${w.port}")
        }
    } catch {
        case e: Throwable =>
        println(s"[Worker] 실패...: ${e.getMessage}")
    } finally {
        channel.shutdown()
    }
}