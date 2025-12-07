import java.nio.file.{Files, Paths, DirectoryStream}
import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.concurrent.{Future, blocking}
import scala.concurrent.ExecutionContext.Implicits.global

import io.grpc.stub.StreamObserver

import com.worker.server.WorkerServer.WorkerServerGrpc.WorkerServer
import com.worker.server.WorkerServer.{Ip, PartitionData, IsAliveReply}
import com.google.protobuf.empty.Empty
import com.google.protobuf.ByteString
import worker.WorkerDashboard

class WorkerServerImpl(tempDir: String) extends WorkerServer {

  @volatile
  private var isPartitionDone: Boolean = false
  @volatile
  private var isDone: Boolean = false

  def markDone(): Unit = {
    isDone = true
  }

  // IP → partition 요청 확인
  private val waitingRequestForGetPartitionData = new ConcurrentHashMap[String, StreamObserver[PartitionData]]()

  private val lock = new Object

  def setPartitionDone(): Unit = {
    val pending = lock.synchronized {
      isPartitionDone = true
      // partition 완료 표시

      // 대기 중이던 getPartitionData 요청들 꺼내기
      val p = waitingRequestForGetPartitionData.asScala.toList
      waitingRequestForGetPartitionData.clear()
      p
    }

    // 각 대기 요청에게 파일 보내기
    pending.foreach { case (ip, observer) => sendPartitionData(ip, observer)}
  }


  /** partition 요청 RPC */
  override def getPartitionData(request: Ip,responseObserver: StreamObserver[PartitionData]): Unit = {
    val ip = request.ip
    if (WorkerDashboard.verbose) {
      WorkerDashboard.WorkerManager.initSending(ip, 0) // 파일 개수는 나중에 채워질 것
    }

    lock.synchronized {
      if (isPartitionDone) {
        sendPartitionData(ip, responseObserver)
      } else {
        waitingRequestForGetPartitionData.put(ip, responseObserver)
      }
    }
  }

  /** 서버 생존 체크 */
  override def isAlive(request: Empty): Future[IsAliveReply] = {
    val reply = IsAliveReply(isAlive = true, isDone = this.isDone)
    Future.successful(reply)
  }

  private def sendPartitionData(ip: String, responseObserver: StreamObserver[PartitionData]): Unit = Future {
    val dir = Paths.get(tempDir)
    var stream: DirectoryStream[java.nio.file.Path] = null

    try {
      // 1. ip-* 패턴에 매칭되는 모든 파일 모으기
      stream = Files.newDirectoryStream(dir, s"$ip-*")
      
      // 2. 파일 이름 순서대로 정렬 (예: IP-0, IP-1, IP-2 순서 보장)
      val filesToStream = stream.iterator().asScala.toList.sortBy(_.getFileName.toString) 
      
      if (filesToStream.isEmpty) {
        responseObserver.onCompleted()
      } else {

        // 3. 찾은 모든 파일을 순서대로 열어서 하나의 스트림으로 전송 (Concatenation)
        if (worker.WorkerDashboard.verbose) {
          worker.WorkerDashboard.WorkerManager.initSending(ip, filesToStream.length)
        }

        blocking {
          filesToStream.foreach { path =>
            var in: InputStream = null
            try {
                if (worker.WorkerDashboard.verbose) {
                  worker.WorkerDashboard.WorkerManager.incSending(ip)
                }

                val fileSize = Files.size(path)
                var nowSent: Long = 0
                in = Files.newInputStream(path)
                val buf = new Array[Byte](100000) // 100KB 버퍼
                var read = in.read(buf)
                while (read != -1) {
                  val chunk = PartitionData(data = ByteString.copyFrom(buf, 0, read))
                  nowSent += read
                  responseObserver.onNext(chunk)
                  read = in.read(buf)
                }
            } finally {
              if (in != null) {
                in.close()
              }
            }
          }
        }


        responseObserver.onCompleted()
      }
    } finally {
      if (stream != null) stream.close()
    }
  }
}
