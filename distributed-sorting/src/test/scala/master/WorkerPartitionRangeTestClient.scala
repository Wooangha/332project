import com.master.server.MasterServer._
import com.master.server.MasterServer.MasterServerGrpc

import io.grpc.ManagedChannelBuilder
import com.google.protobuf.ByteString

object WorkerPartitionRangeTestClient extends App {

    // args:
    // 0: workerId (0, 1, 2 ...)
    // 1: master host (optional, default "localhost")
    // 2: master port (optional, default 50057)
    val workerId: Int =
        if (args.length >= 1) args(0).toInt else 0

    val host: String =
        if (args.length >= 2) args(1) else "localhost"

    val port: Int =
        if (args.length >= 3) args(2).toInt else 50057

    // 테스트용 worker ip 문자열 (실제 IP 아니어도 됨, 구분만 되면 OK)
    val workerIp: String = s"worker-$workerId"

    println(s"[Worker $workerId] connecting to master at $host:$port (ip=$workerIp)")

    val channel = ManagedChannelBuilder
        .forAddress(host, port)
        .usePlaintext()
        .build()

    val bloc = MasterServerGrpc.blockingStub(channel)

    try {
        // 1) 이 워커가 보낼 샘플 키 50개 생성 (길이 10 bytes)
        val sampleKeyBytes: Seq[Array[Byte]] =
        makeSampleKeys(workerId, count = 50, keyLen = 10)

        // 2) SampleKeyData.Key 메시지로 변환
        val keyMessages: Seq[SampleKeyData.Key] =
        sampleKeyBytes.map { arr =>
            SampleKeyData.Key(
            keyDatum = ByteString.copyFrom(arr)
            )
        }

        // 3) ip 필드까지 채워서 요청 생성
        val request = SampleKeyData(
        ip = workerIp,
        keyData = keyMessages
        )

        println(s"[Worker $workerId] Call getPartitionRange with ${keyMessages.size} sample keys")

        // 4) 동기 호출
        val reply: PartitionRanges = bloc.getPartitionRange(request)

        // 5) 받은 파티션 범위 출력
        println(s"[Worker $workerId] Received ${reply.partitionRanges.size} partition ranges:")
        reply.partitionRanges.zipWithIndex.foreach {
            case (pr, idx) => {
                val startHex = bytesToHex(pr.startKey.toByteArray)
                val endHex   = bytesToHex(pr.endKey.toByteArray)
                println(s"  - Partition $idx: [ $startHex , $endHex )")
            }
        }

    } catch {
        case e: Throwable =>
        println(s"[Worker $workerId] getPartitionRange 실패...: ${e.getMessage}")
    } finally {
        channel.shutdown()
    }

    // ---------- helpers ----------

    private def makeSampleKeys(workerId: Int, count: Int, keyLen: Int): Seq[Array[Byte]] = {
        (0 until count).map { idx =>
        val base = (workerId * 50 + idx) % 256  // 0~255 사이 값
        val bytes = Array.tabulate[Byte](keyLen) { i =>
            ((base + i) % 256).toByte
        }
        bytes
        }
    }

    private def bytesToHex(bytes: Array[Byte]): String =
        bytes.map(b => f"${b & 0xff}%02X").mkString(" ")
}