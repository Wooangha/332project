package com.worker.server

import com.worker.server.WorkerServerGrpc.WorkerServer
import com.worker.server.WorkerServer.{Ip, PartitionData, IsAliveReply}
import com.google.protobuf.ByteString

import io.grpc.stub.StreamObserver
import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentHashMap
import java.nio.file.{Files, Paths}
import java.io.InputStream
import com.google.protobuf.empty.Empty
import scala.concurrent.Future

class WorkerServerImpl(tempDir: String)
    extends WorkerServer {

    @volatile
    private var isPartitionDone: Boolean = false

    // IP → partition 요청 확인
    private val waitingRequestForGetPartitionData = new ConcurrentHashMap[String, StreamObserver[PartitionData]]()

    private val lock = new Object

    private def setPartitionDone(): Unit = {
        println("[WorkerServer] Partition 완료됨")
        // partition 완료 표시
        isPartitionDone = true

        // 대기 중이던 getPartitionData 요청들 꺼내기
        val pending = waitingRequestForGetPartitionData.asScala.toList
        waitingRequestForGetPartitionData.clear()

        // 각 대기 요청에게 파일 보내기
        pending.foreach { case (ip, observer) => sendPartitionData(ip, observer)}
    }


    /** partition 요청 RPC */
    override def getPartitionData(request: Ip,responseObserver: StreamObserver[PartitionData]): Unit = {

        val ip = request.ip
        println(s"[WorkerServer] getPartitionData 요청 받음: from=$ip")

        lock.synchronized {
            if (isPartitionDone) {
                println("[WorkerServer] Partition 끝! 즉시 전송")
                sendPartitionData(ip, responseObserver)} 
            else {
                println(s"[WorkerServer] Partition 미완료 → 대기 리스트로 저장: $ip")
                waitingRequestForGetPartitionData.put(ip, responseObserver)}
        }
    }

    /** 서버 생존 체크 */
    override def isAlive(request: Empty): Future[IsAliveReply] = {
        println("[WorkerServer] isAlive called")
        Future.successful(IsAliveReply(isAlive = true))
        }


    /** 내부 함수: 파일 스트리밍 */
    private def sendPartitionData(ip: String,responseObserver: StreamObserver[PartitionData]): Unit = {

        val filePath = Paths.get(tempDir, s"$ip-partition.bin")

        if (!Files.exists(filePath)) {
            
            println(s"[WorkerServer] ERROR: partition 파일 없음: $filePath")
            responseObserver.onError(new RuntimeException(s"No partition file for ip=$ip"))
            return
        }

        println(s"[WorkerServer] partition 파일 스트리밍 시작: $filePath")

        var in: InputStream = null

        try {
            in = Files.newInputStream(filePath)
            val buffer = new Array[Byte](1024 * 1024)

            var read = in.read(buffer)
            while (read != -1) {
                val chunk = PartitionData(data = ByteString.copyFrom(buffer, 0, read))
                responseObserver.onNext(chunk)
                read = in.read(buffer)
            }

            responseObserver.onCompleted()
            println(s"[WorkerServer] partition 파일 전송 완료: $ip")} 
        catch {case e: Exception =>
                println("[WorkerServer] ERROR: 전송 중 예외 발생")
                responseObserver.onError(e)} 
        finally {if (in != null) in.close()}
    }
}
