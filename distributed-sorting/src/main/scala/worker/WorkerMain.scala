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

//below for shuffle stage(fault-tolerance)
import java.util.concurrent.TimeUnit
import io.grpc.StatusRuntimeException

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

        var currentVersion = initReply.version.map(_.version).getOrElse(0)
        var currentWorkerInfos: Seq[WorkerInfo] = initReply.workerInfos
        val myWorkerInfo: WorkerInfo = WorkerInfo(ip = workerIp, port = workerPort)

        println(s"[Worker] registered. version=$currentVersion")
        println("[Worker] worker list from master:")
        currentWorkerInfos.foreach { w =>
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
            currentWorkerInfos.map(_.ip).toList
        
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
            myWorkerInfo = myWorkerInfo,
            masterStub = masterbloc,
            initialWorkers = currentWorkerInfos,
            initialVersion = currentVersion
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
     */ // gpt가 일단 셔플 관련 로직으로 코드 먼저 생성하고, 내가 추가로 타임 아웃 + 에러 감지로 튜닝 업글 해봄
    private def fetchAllPartitionsForMe(
        myIp: String,
        myWorkerInfo: WorkerInfo,
        masterStub: MasterServerGrpc.MasterServerBlockingStub,
        initialWorkers: Seq[WorkerInfo],
        initialVersion: Int
    ): Future[List[String]] = {
        // 어떤 ip들 한테서 받아야 하는지(지 자신도 포함임 -> 추후 최적화 해도 될 듯)
        val targetIps: Seq[String] = initialWorkers.map(_.ip).distinct

        // 각 ip 별로 병렬 셔플 + 재시도
        val perIpFutures: Seq[Future[List[String]]] = targetIps.map { targetIp =>
            Future {
                var curWorkers: Seq[WorkerInfo] = initialWorkers
                var curVersion: Int = initialVersion

                def refreshFromMaster(): Unit = {
                    println(s"[Worker] shuffle-register for targetIp=$targetIp")
                    
                    val req = RegisterRequest(
                        workerInfo = Some(myWorkerInfo),
                        isShuffle = true
                    )
                    val reply = masterStub.register(req)

                    val newVer = reply.version.map(_.version).getOrElse(curVersion)
                    if(newVer != curVersion) {
                        println(s"[Worker] version updated $curVersion -> $newVer")
                        curVersion = newVer
                    } else {
                        println(s"[Worker] version unchanged = $curVersion")
                    }

                    curWorkers = reply.workerInfos
                }

                def findWorker(ip: String): Option[WorkerInfo] = 
                    curWorkers.find(_.ip == ip)
                
                def fetchOnceFromWorker(target: WorkerInfo): Either[Throwable, List[String]] = {
                    val ip = target.ip
                    val port = target.port

                    var cnt = 0
                    def makeTmpPath(): String = {
                      cnt += 1
                      Paths.get(
                        DataProcessor.tempDirPrefix,
                        s"shuffle_from_${ip}_$port-$cnt").toString
                    }

                    println(s"[Worker] try fetch partition for $myIp from $ip:$port")

                    val channel = ManagedChannelBuilder
                        .forAddress(ip, port)
                        .usePlaintext()
                        .build()
                    
                    try {
                        val stub = WorkerServerGrpc
                            .blockingStub(channel)
                            .withDeadlineAfter(2, TimeUnit.MINUTES) // silent-hang 방지용인데, 시간 이거 좀 적절히 조절해야 할 듯?
                        
                        val req = com.worker.server.WorkerServer.Ip(ip = myIp)
                        val it = stub.getPartitionData(req) // blocking iterator

                        var savePaths = List[String]()
                        for (partData <- it) {
                            val tmpPath = makeTmpPath()
                            val outPath = Paths.get(tmpPath)
                            savePaths = tmpPath :: savePaths
                            val out = java.nio.file.Files.newOutputStream(outPath)
                            try {
                                out.write(partData.data.toByteArray)
                            } finally {
                                out.close()
                            }
                        }

                        println(s"[Worker] fetch from $ip:$port succeed")
                        Right(savePaths)

                    } catch {
                        case e: Throwable =>
                            /* 반쯤 받은 파일은 다음 시도를 위해 삭제하는 방식의 코드
                            try java.nio.file.Files.deleteIfExists(Paths.get(tmpPath))
                            catch { case _: Throwable => () }
                            */
                            e match {
                                case sre: StatusRuntimeException => // grpc 에러만 따로 골라 잡기 히히
                                    println(s"[Worker] gRPC error from $ip:$port - status=${sre.getStatus}, desc=${sre.getMessage}")
                                case other =>
                                    println(s"[Worker] unexpected error from $ip:$port - ${other.getClass.getSimpleName}: ${other.getMessage}")
                            }
                            Left(e)
                    } finally {
                        channel.shutdown()
                    }
                }

                var done = false
                var lastPath: Option[List[String]] = None

                while (!done) {
                    val targetWorker = findWorker(targetIp)

                    fetchOnceFromWorker(targetWorker.get) match {
                        case Right(path) => 
                            lastPath = Some(path)
                            done = true

                        case Left(_) =>
                            println(s"[Worker] fetch from ${targetWorker.get.ip}:${targetWorker.get.port} failed. " + s"register(isShuffle=true) 후 재시도.")
                            refreshFromMaster()
                            Thread.sleep(1000L)
                    }
                }

                lastPath.get
            }
        }

        Future.sequence(perIpFutures).map(_.toList.flatten)
    }

}