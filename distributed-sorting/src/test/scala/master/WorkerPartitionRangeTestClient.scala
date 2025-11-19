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

    println(s"[Worker $workerId] connecting to master at $host:$port")

    val channel = ManagedChannelBuilder
        .forAddress(host, port)
        .usePlaintext()
        .build()

    val bloc = MasterServerGrpc.blockingStub(channel)

    try {
        // 1) 이 워커가 보낼 샘플 키 50개 생성 (길이 10 bytes)
        //    workerId 마다 다른 값이 나오도록 base 값을 다르게 줌
        val sampleKeyBytes: Seq[Array[Byte]] = makeSampleKeys(workerId, count = 50, keyLen = 10)

        // 2) SampleKeyData 메시지로 변환
        val keyMessages: Seq[SampleKeyData.Key] =
        sampleKeyBytes.map { arr =>
            SampleKeyData.Key(
            keyDatum = ByteString.copyFrom(arr)
            )
        }

        val request = SampleKeyData(keyData = keyMessages)

        println(s"[Worker $workerId] Call getPartitionRange with ${keyMessages.size} sample keys")

        // 3) 동기 호출
        val reply: PartitionRanges = bloc.getPartitionRange(request)

        // 4) 받은 파티션 범위 출력
        println(s"[Worker $workerId] Received ${reply.partitionRanges.size} partition ranges:")
        reply.partitionRanges.zipWithIndex.foreach { case (pr, idx) =>
        val startHex = bytesToHex(pr.startKey.toByteArray)
        val endHex   = bytesToHex(pr.endKey.toByteArray)
        println(s"  - Partition $idx: [ $startHex , $endHex )")
        }

    } catch {
        case e: Throwable =>
        println(s"[Worker $workerId] getPartitionRange 실패...: ${e.getMessage}")
    } finally {
        channel.shutdown()
    }

    // ---------- helpers ----------

    // workerId, count, keyLen에 따라 고정된 패턴의 키 생성
    // 예: workerId=0 → 0~49, workerId=1 → 50~99 이런 식으로 값이 달라짐
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