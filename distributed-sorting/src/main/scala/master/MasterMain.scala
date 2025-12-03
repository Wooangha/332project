import com.master.server.MasterServer.MasterServerGrpc

import io.grpc.netty.NettyServerBuilder
import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.ExecutionContextExecutor

import java.net.{InetAddress, Inet4Address, NetworkInterface}
import scala.jdk.CollectionConverters._


object MasterMain extends App {
    implicit val ec:ExecutionContext = ExecutionContext.global

    val NUM_OF_WORKER = args(0).toInt

    //마스터 종료 기다리는 promise
    val masterShutdownPromise = Promise[Unit]()

    val server = NettyServerBuilder
        .forPort(0)
        .addService(MasterServerGrpc.bindService(new MasterServerImpl(NUM_OF_WORKER, onMasterCanShutdown = () => masterShutdownPromise.trySuccess(())), ec))
        .build.start()

    def getMyIp: String = {
        NetworkInterface.getNetworkInterfaces.asScala
            .flatMap(_.getInetAddresses.asScala)
            .filter(addr =>
            !addr.isLoopbackAddress &&
            addr.isInstanceOf[Inet4Address] &&
            !addr.getHostAddress.startsWith("127")
            )
            .map(_.getHostAddress)
            .toList
            .headOption
            .getOrElse(InetAddress.getLocalHost.getHostAddress)
    }

    val port = server.getPort()
    val ip = getMyIp

    println(s"$ip:$port")

    // MasterServerImpl에서 promise complete되면 서버 닫음
    masterShutdownPromise.future.foreach { _ =>
        server.shutdown()
    }

    server.awaitTermination()
}