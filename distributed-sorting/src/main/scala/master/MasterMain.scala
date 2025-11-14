import com.master.server.MasterServer.MasterServerGrpc
import io.grpc.netty.NettyServerBuilder
import scala.concurrent.{ExecutionContext}
import scala.concurrent.ExecutionContextExecutor

object MasterMain extends App {
    implicit val ec:ExecutionContext = ExecutionContext.global

    private val port = 50055

    val server = NettyServerBuilder
    .forPort(port)
    .addService(MasterServerGrpc.bindService(new MasterServerImpl, ec))
    .build.start()

    println(s"[Master] Server Started on port: $port")

    /*
    sys.addShutdownHook{
        println("[Master] Server Shutdown...")
        server.shutdown()
    }
    */

    server.awaitTermination()
}