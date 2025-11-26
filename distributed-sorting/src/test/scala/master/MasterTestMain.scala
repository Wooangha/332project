import com.master.server.MasterServer.MasterServerGrpc
import io.grpc.netty.NettyServerBuilder
import scala.concurrent.{ExecutionContext}
import scala.concurrent.ExecutionContextExecutor

object MasterTestMain extends App {
    implicit val ec:ExecutionContext = ExecutionContext.global

    private val port = 50057

    val server = NettyServerBuilder
    .forPort(port)
    .addService(MasterServerGrpc.bindService(new MasterServerImpl(3), ec))
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