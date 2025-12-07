package worker
// ============================================================
// WorkerPipelineDashboard.scala (v4)
//  - Status enum 기반으로 setStatus 적용
//  - WORKERS 위치 최상단, | 제거, 전체 출력 포맷 정리
// ============================================================

object WorkerDashboard {
  var verbose: Boolean = false

  // ============================================================
  // Pipeline Status Enum (사용자가 선택)
  // ============================================================
  sealed trait PipelineStatus
  case object Registering extends PipelineStatus
  case object Sampling extends PipelineStatus
  case object SortPartitioning extends PipelineStatus
  case object Shuffling extends PipelineStatus
  case object Merging extends PipelineStatus
  case object ShutdownWaiting extends PipelineStatus
  case object ShutdownRetrying extends PipelineStatus
  case object Done extends PipelineStatus

  @volatile
  private var currentStatus: PipelineStatus = Registering

  def setStatus(s: PipelineStatus): Unit = synchronized {
    currentStatus = s
  }

  // ============================================================
  // Shuffle Status
  // ============================================================
  sealed trait ShuffleStatus
  case object SHUFFLE_NORMAL extends ShuffleStatus
  case object SHUFFLE_CONNECTION_FAILED extends ShuffleStatus
  case object SHUFFLE_UPDATING_INFO extends ShuffleStatus

  // ============================================================
  // Worker Info
  // ============================================================
  case class WorkerInfo(ip: String, port: Int)

  // ============================================================
  // 세부 상태 구조체
  // ============================================================
  case class SortPartitionState(total: Int = 0, current: Int = 0)
  case class SendingInfo(current: Int = 0, max: Int = 0, status: ShuffleStatus = SHUFFLE_NORMAL)
  case class ReceivingInfo(current: Int = 0, status: ShuffleStatus = SHUFFLE_NORMAL)
  case class MergingState(total: Int = 0, current: Int = 0)

  // ============================================================
  // 전체 Pipeline 상태
  // ============================================================
  case class WorkerPipelineState(
    workers: Map[String, WorkerInfo] = Map.empty,
    sortPartition: SortPartitionState = SortPartitionState(),
    sending: Map[String, SendingInfo] = Map.empty,
    receiving: Map[String, ReceivingInfo] = Map.empty,
    merging: MergingState = MergingState()
  )

  // ============================================================
  // Manager
  // ============================================================
  object WorkerManager {

    private var state: WorkerPipelineState = WorkerPipelineState()
    def getState: WorkerPipelineState = state

    // ---------- WORKERS ----------
    def initWorkers(ws: Seq[(String, Int)]): Unit = synchronized {
      state = state.copy(workers = ws.map { case (ip, port) => ip -> WorkerInfo(ip, port) }.toMap)
    }

    // ---------- SORT ----------
    def initSortPartitioning(total: Int): Unit = synchronized {
      state = state.copy(sortPartition = SortPartitionState(total, 0))
    }
    def incSortPartitioning(): Unit = synchronized {
      val s = state.sortPartition
      state = state.copy(sortPartition = s.copy(current = s.current + 1))
    }

    // ---------- SENDING ----------
    def initSending(ip: String, max: Int): Unit = synchronized {
      state = state.copy(sending = state.sending + (ip -> SendingInfo(0, max)))
    }
    def incSending(ip: String, delta: Int = 1): Unit = synchronized {
      val s = state.sending.getOrElse(ip, SendingInfo())
      state = state.copy(sending = state.sending + (ip -> s.copy(current = s.current + delta)))
    }
    def setSendingFailed(ip: String): Unit = synchronized {
      val s = state.sending.getOrElse(ip, SendingInfo())
      state = state.copy(sending = state.sending + (ip -> s.copy(status = SHUFFLE_CONNECTION_FAILED)))
    }

    // ---------- RECEIVING ----------
    def initReceiving(ip: String): Unit = synchronized {
      state = state.copy(receiving = state.receiving + (ip -> ReceivingInfo()))
    }
    def incReceiving(ip: String, delta: Int = 1): Unit = synchronized {
      val r = state.receiving.getOrElse(ip, ReceivingInfo())
      state = state.copy(receiving = state.receiving + (ip -> r.copy(current = r.current + delta)))
    }
    def setReceivingFailed(ip: String): Unit = synchronized {
      val r = state.receiving.getOrElse(ip, ReceivingInfo())
      state = state.copy(receiving = state.receiving + (ip -> r.copy(status = SHUFFLE_CONNECTION_FAILED)))
    }

    // ---------- MERGING ----------
    def initMerging(total: Int): Unit = synchronized {
      state = state.copy(merging = MergingState(total, 0))
    }
    def incMerging(): Unit = synchronized {
      val m = state.merging
      state = state.copy(merging = m.copy(current = m.current + 1))
    }
  }

  // ============================================================
  // Progress Bar
  // ============================================================
  def progressBar(cur: Int, total: Int, width: Int = 20): String = {
    if (total == 0) return "-"
    val pct = (cur.toDouble / total * 100).toInt
    val filled = (pct * width) / 100
    val empty = width - filled
    val bar = "[" + "=" * filled + (if (filled < width) ">" else "") + " " * (empty - 1).max(0) + "]"
    f"$pct%3d%% $bar $cur / $total Partitions"
  }

  // ============================================================
  // Dashboard Builder
  // ============================================================
  def buildDashboard(st: WorkerPipelineState): String = {

    val statusText = currentStatus match {
      case Registering => "REGISTERING"
      case Sampling => "SAMPLING"
      case SortPartitioning => "SORT & PARTITIONING"
      case Shuffling => "SHUFFLING"
      case Merging => "MERGING"
      case ShutdownWaiting => "SHUTDOWN WAITING"
      case ShutdownRetrying => "SHUTDOWN RETRYING"
      case Done => "DONE"
    }

    val workers =
      if (st.workers.isEmpty) "WORKERS:\n  -"
      else
        "WORKERS:\n" +
        st.workers.values.toSeq
          .sortBy(_.ip)
          .map(w => s"  - ${w.ip}:${w.port}")
          .mkString("\n")

    val sort =
      if (st.sortPartition.total == 0) "SORT & PARTITIONING:\n  -"
      else s"SORT & PARTITIONING:\n  ${progressBar(st.sortPartition.current, st.sortPartition.total)}"

    val sending =
      if (st.sending.isEmpty)
        "  SENDING:\n    -"
      else {
        val body =
          st.sending.toSeq.sortBy(_._1).map { case (ip, info) =>
            info.status match {
              case SHUFFLE_CONNECTION_FAILED => s"    $ip: CONNECTION FAILED"
              case SHUFFLE_UPDATING_INFO     => s"    $ip: UPDATING INFO"
              case SHUFFLE_NORMAL =>
                val pct = if (info.max == 0) 0 else (info.current.toDouble / info.max * 100).toInt
                s"    $ip: Sent $pct% (${info.current}/${info.max})"
            }
          }.mkString("\n")
        s"  SENDING:\n$body"
      }

    val receiving =
      if (st.receiving.isEmpty)
        "  RECEIVING:\n    -"
      else {
        val body =
          st.receiving.toSeq.sortBy(_._1).map { case (ip, info) =>
            info.status match {
              case SHUFFLE_CONNECTION_FAILED => s"    $ip: CONNECTION FAILED"
              case SHUFFLE_UPDATING_INFO     => s"    $ip: UPDATING INFO"
              case SHUFFLE_NORMAL =>
                if (info.current == 0) s"    $ip: -" else s"    $ip: Received ${info.current}"
            }
          }.mkString("\n")
        s"  RECEIVING:\n$body"
      }

    val merging =
      if (st.merging.total == 0 && st.merging.current == 0)
        "MERGING:\n  WRITING: -"
      else
        s"MERGING:\n  WRITING: ${st.merging.current}"

    raw"""
======================================================================
Status: $statusText

$workers

$sort

SHUFFLING:
$sending
$receiving

$merging
======================================================================
""".stripMargin
  }

  // ============================================================
  // Renderer
  // ============================================================
  object ConsoleCtrl {
    def moveUp(n: Int) = s"\u001b[${n}A"
    val clearLine = "\u001b[2K"
  }

  object DashboardRenderer {

    import ConsoleCtrl._
    private var printedLines = 0

    def renderOnce(): Unit = {
      val text = buildDashboard(WorkerManager.getState)
      val lines = text.count(_ == '\n')

      if (printedLines > 0) {
        print(moveUp(printedLines))
        for (_ <- 0 until printedLines) print(clearLine + "\n")
        print(moveUp(printedLines))
      }

      print(text)
      printedLines = lines
    }

    def start(intervalMs: Int = 500): Unit = {
      new Thread(() => {
        while (true) {
          renderOnce()
          Thread.sleep(intervalMs)
        }
      }).start()
    }
  }
}

import WorkerDashboard._
import WorkerDashboard.WorkerManager._

object ExamplePipelineSimulation {

  def main(args: Array[String]): Unit = {
    // 대시보드 시작
    DashboardRenderer.start(300)

    // ----------------------------------------
    // 1) REGISTERING 단계
    // ----------------------------------------
    setStatus(Registering)

    initWorkers(Seq(
      ("1.1.1.1", 7000),
      ("2.2.2.2", 7001),
      ("10.0.0.1", 7002)
    ))

    Thread.sleep(1500)

    // ----------------------------------------
    // 2) SAMPLING 단계
    // ----------------------------------------
    setStatus(Sampling)

    // 샘플링이라고 가정하고 단순한 대기
    Thread.sleep(1000)

    // ----------------------------------------
    // 3) SORT & PARTITIONING 단계
    // ----------------------------------------
    setStatus(SortPartitioning)

    initSortPartitioning(100)

    for (_ <- 1 to 60) {
      incSortPartitioning()
      Thread.sleep(50)
    }

    Thread.sleep(500)

    for (_ <- 61 to 100) {
      incSortPartitioning()
      Thread.sleep(20)
    }

    // ----------------------------------------
    // 4) SHUFFLING 단계
    // ----------------------------------------
    setStatus(Shuffling)

    // SENDING 초기화
    initSending("1.1.1.1", 120)
    initSending("2.2.2.2", 90)
    initSending("10.0.0.1", 150)

    // RECEIVING 초기화
    initReceiving("1.1.1.1")
    initReceiving("2.2.2.2")
    initReceiving("10.0.0.1")

    // Sending / Receiving 진행
    for (i <- 1 to 120) {
      if (i <= 120) incSending("1.1.1.1")
      if (i <= 90) incSending("2.2.2.2")
      if (i <= 150) incSending("10.0.0.1")

      // Receiving은 Random 값으로 시뮬
      if (scala.util.Random.nextInt(4) == 0) incReceiving("1.1.1.1")
      if (scala.util.Random.nextInt(6) == 0) incReceiving("2.2.2.2")
      if (scala.util.Random.nextInt(8) == 0) incReceiving("10.0.0.1")

      if (i == 30) setSendingFailed("2.2.2.2")      // 장애 발생 예시
      if (i == 60) setReceivingFailed("10.0.0.1")   // 장애 발생 예시

      Thread.sleep(40)
    }

    Thread.sleep(1000)

    // ----------------------------------------
    // 5) MERGING 단계
    // ----------------------------------------
    setStatus(Merging)

    initMerging(5000)

    for (_ <- 1 to 3000) {
      incMerging()
      Thread.sleep(1)
    }

    Thread.sleep(1000)

    for (_ <- 3001 to 5000) {
      incMerging()
    }

    Thread.sleep(1000)

    // ----------------------------------------
    // 6) DONE
    // ----------------------------------------
    setStatus(Done)

    Thread.sleep(2000)

    println("\n\nPipeline Finished Successfully 🎉")
  }
}
