import com.master.MasterServer._
import com.master.MasterServerGrpc.MasterServerGrpc

import io.grpc.ManagedChannelBuilder
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}

object WorkerRegisterTestClient extends App {
    implicit val ec: ExecutionContext = ExecutionContext.global

    val ipArg = args(0)
    val portArg = args(1).toInt

    

}