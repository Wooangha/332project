import java.net.{Inet4Address, InetAddress, NetworkInterface}
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import io.grpc.netty.NettyServerBuilder
import io.grpc.ManagedChannelBuilder
import scala.concurrent.blocking

import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty

import common.{Key, Data}
import worker.DataProcessor
import worker.Util

// master proto
import com.master.server.MasterServer._
import com.master.server.MasterServer.MasterServerGrpc

// worker proto
import com.worker.server.WorkerServer._
import com.worker.server.WorkerServer.WorkerServerGrpc

object WorkerMain extends App {
    implicit val ec: ExecutionContext = ExecutionContext.global

    // ----------------- 0. 인자 파싱 -----------------
    val masterAddr = args(0)
    val Array(masterHost, masterPortStr) =
        masterAddr.split(":", 2) match {
            case Array(h, p) => Array(h, p)
        }
    val masterPort = masterPortStr.toInt

    val (inputDirs, outputDirs) = parseIOArgs(args.drop(1))

    //출력 디렉토리 생성
    Files.createDirectories(Paths.get(outputDirs))

    // ----------------- 1. 워커 서버 시작 -----------------
    val workerServiceImpl = new WorkerServerImpl(DataProcessor.tempDirPrefix) // Q.인자 이거 맞음?

    val workerServer = NettyServerBuilder
        .forPort(0)                 // OS가 포트 할당
        .addService(WorkerServerGrpc.bindService(workerServiceImpl, ec))
        .build().start()
    
    val workerPort = workerServer.getPort
    val workerIp = getMyIp

    // ----------------- 2. 마스터 채널 / 스텁 -----------------
    val masterChannel = ManagedChannelBuilder
        .forAddress(masterHost, masterPort)
        .usePlaintext()
        .build()
    
    val masterbloc = MasterServerGrpc.blockingStub(masterChannel)

    try {
        // ----------------- 3. register(init) -----------------
        println(s"[Worker] register(init) to master")

        val initReq = RegisterRequest(
            workerInfo = Some(WorkerInfo(ip = workerIp, port = workerPort)),
            isShuffle = false
        )

        val initReply: RegisterReply = masterbloc.register(initReq)

        val version0 = initReply.version.map(_.version).getOrElse(0)
        val workerInfos: Seq[WorkerInfo] = initReply.workerInfos

        println(s"[Worker] registered. version=$version0")
        println("[Worker] worker list from master:")
        workerInfos.foreach { w =>
            println(s"  - ${w.ip}:${w.port}")
        }

        // ----------------- 4. 샘플링 + getPartitionRange -----------------
        val sampleSize = 10000

        // 비동기 처리 맞는지?
        val sampleKeysF: Future[Array[Key]] = DataProcessor
            .sampling(inputDirs.toList, sampleSize)

        val sampleKeys: Array[Key] = Await.result(sampleKeysF, Duration.Inf) // 코드 이렇게 써도 안전한가?

        val keyMessages: Seq[SampleKeyData.Key] = sampleKeys.map {
            k => SampleKeyData.Key(keyDatum = ByteString.copyFrom(k.key))
        }

        val sampleReq = SampleKeyData(ip = workerIp, keyData = keyMessages)

        println(s"[Worker] sending ${keyMessages.size} sample keys to master")

        val partitionRangesMsg: PartitionRanges = masterbloc.getPartitionRange(sampleReq)

        val partitionRangesKey: List[(Key, Key)] = 
            partitionRangesMsg.partitionRanges.toList.map {
                pr => (Key(pr.startKey.toByteArray), Key(pr.endKey.toByteArray))
            }

        println(s"[Worker] received ${partitionRangesKey.size} partition ranges")

        // ----------------- 5. 파티션 함수 생성 -----------------
        val workerIpList: List[String] = 
            workerInfos.map(_.ip).toList
        
        val partitionFunc: Key => String = Util.makePartition(partitionRangesKey, workerIpList)

        // ----------------- 6. 로컬 sort + partitioning -----------------
        println("[Worker] sort and partitioning local data start")

        val partitionedDirsF: Future[List[String]] = 
            DataProcessor.sortAndPartitioning(inputDirs.toList, partitionFunc)
        
        val partitionedDirs: List[String] = Await.result(partitionedDirsF, Duration.Inf)

        println("[Worker] local sort+partitioning done. Notify worker-server that partitions are ready.")
        workerServiceImpl.setPartitionDone()

        // ----------------- 7. 셔플 단계: 다른 워커에게서 우리 파티션 받기 -----------------
        println("[Worker] shuffle: fetching my partitions from all workers")

        val myIp = workerIp

        val remotePartitionFilesF: Future[List[String]] = fetchAllPartitionsForMe(
            myIp = myIp,
            workers = workerInfos,
            outputDir = outputDirs
        )

        val remotePartitionFiles: List[String] = Await.result(remotePartitionFilesF, Duration.Inf)

        println(s"[Worker] shuffle done. Collected ${remotePartitionFiles.size} temp files for my partitions")

        // ----------------- 8. 머지 + 최종 출력 -----------------
        var partIdx = 0
        def makeOutputDir(): String = {
            val path = Paths.get(outputDirs, s"partition.$partIdx")
            partIdx += 1
            path.toString()
        }

        // 하나의 출력 파일에 몇 개 레코드까지 넣을지 -> 추후 조정 필요
        val maxRecordsPerFile = 320000

        println("[Worker] merging collected partitions into final output files")

        DataProcessor.merge(
            dataDirLs = remotePartitionFiles,
            makeNewDir = makeOutputDir,
            maxSize = maxRecordsPerFile
        )

        println("[Worker] merge done. Final outputs:")
        (0 until partIdx).foreach { i =>
            println(s"  - ${Paths.get(outputDirs, s"partition.$i")}")
        }

        // ----------------- 9. WorkerServerImpl 에 '끝났다' 표시 -----------------

        workerServiceImpl.markDone()
        
        // ----------------- 10. canShutdownWorkerServer 루프 -----------------
        println("[Worker] asking master whether I can shutdown")

        val myIpMsg = com.master.server.MasterServer.Ip(ip = workerIp)

        var canShutdown = false
        while(!canShutdown) {
            val reply: CanShutdownWorkerServerReply = masterbloc.canShutdownWorkerServer(myIpMsg)

            if(reply.canShutdownWorkerServer) {
                println("[Worker] master says I can shutdown. Exiting.")
                canShutdown = true
            } else {
                println("[Worker] master says not yet. Will retry...")

                // 일단은 시간 걸어둠. 어차피 웨리포에 담겨서 굳이긴 한데..
                Thread.sleep(3000)
            }
        }


    } finally {
        // 위에서 오류가 쳐 나든 말든 일단 채널/서버 정리하기
        masterChannel.shutdown()
        workerServer.shutdown()
        workerServer.awaitTermination()
    }

    // below for helper functions

    // args: '-I d1 d2 ... -O outDir' 형태를 파싱하는 함수(by GPT)
    private def parseIOArgs(rest: Array[String]): (List[String], String) = {
        val inputs    = ListBuffer[String]()
        var outputOpt = Option.empty[String]

        var i = 0
        while (i < rest.length) {
            rest(i) match {
                case "-I" =>
                    i += 1
                    while (i < rest.length && rest(i) != "-O") {
                        inputs += rest(i)
                        i += 1
                    }
                case "-O" =>
                    if (i + 1 >= rest.length) {
                        System.err.println("'-O' must be followed by an output directory")
                        sys.exit(1)
                    }
                    outputOpt = Some(rest(i + 1))
                    i += 2
                case other =>
                    System.err.println(s"Unknown argument: $other")
                    sys.exit(1)
            }
        }

        if (inputs.isEmpty) {
            System.err.println("At least one input directory must be given after -I")
            sys.exit(1)
        }
        if (outputOpt.isEmpty) {
            System.err.println("Output directory must be given with -O")
            sys.exit(1)
        }

        (inputs.toList, outputOpt.get)
    }

    // 현재 머신의 IPv4 주소 하나 리턴 (MasterMain 과 같은 방식) (by GPT)
    private def getMyIp: String = {
        NetworkInterface.getNetworkInterfaces.asScala
        .flatMap(_.getInetAddresses.asScala)
        .collectFirst {
            case addr: Inet4Address
            if !addr.isLoopbackAddress && !addr.getHostAddress.startsWith("127") =>
            addr.getHostAddress
        }
        .getOrElse(InetAddress.getLocalHost.getHostAddress)
    }
    
    /*  below helper function is created by GPT
        1.	모든 워커에서 “나에게 속한” 파티션 조각들을 받아와서
        2.	remotePartitionFiles 리스트에 담고
        3.	그걸 merge 해서 최종 정렬된 결과를 여러 개의 output 파일로 쓰는 단계야.
     */
    private def fetchAllPartitionsForMe(
        myIp: String,
        workers: Seq[WorkerInfo],
        outputDir: String
    ): Future[List[String]] = {
        val futures = workers.map { wInfo =>
            Future {
                val ip   = wInfo.ip
                val port = wInfo.port

                val tmpPath = Paths.get(outputDir, s"shuffle_from_${sanitize(ip)}_$port").toString

                println(s"[Worker] fetching partition for $myIp from $ip:$port -> $tmpPath")

                val channel = ManagedChannelBuilder
                    .forAddress(ip, port)
                    .usePlaintext()
                    .build()

                try {
                val stub = WorkerServerGrpc.blockingStub(channel)

                val req  = com.worker.server.WorkerServer.Ip(ip = myIp)
                val it   = stub.getPartitionData(req) // blocking iterator

                // 여기서는 단순히 받은 바이트를 그대로 파일에 써 둠
                val out = java.nio.file.Files.newOutputStream(Paths.get(tmpPath))
                try {
                    while (it.hasNext) {
                        val partData = it.next()
                        val bytes = partData.data.toByteArray
                        out.write(bytes)
                    }
                } finally {
                    out.close()
                }

                tmpPath
                } finally {
                    channel.shutdown()
                }
            }
        }

    Future.sequence(futures).map(_.toList)
    }

    private def sanitize(ip: String): String = ip.replace(":", "_").replace(".", "_")
    
}