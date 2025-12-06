// ============================================================
// MasterDashboard.scala
//   - Master의 REGISTERING / SAMPLING / SHUTDOWN 상태 관리 + 대시보드
// ============================================================

object MasterDashboard {
  var verbose: Boolean = false
  // ============================================================
  // Master Phase
  // ============================================================
  sealed trait MasterPhase
  case object REGISTERING extends MasterPhase
  case object SAMPLING extends MasterPhase
  case object SHUTDOWN extends MasterPhase
  case object DONE extends MasterPhase

  // ============================================================
  // Worker Info / Shutdown Info
  // ============================================================
  case class WorkerInfo(ip: String, port: Int)

  sealed trait ShutdownState
  case object NOT_YET extends ShutdownState
  case object SHUTDOWN_RECEIVED extends ShutdownState
  case object SHUTDOWN_DELAYED extends ShutdownState

  // ============================================================
  // Immutable MasterState
  // ============================================================
  case class MasterState(
    phase: MasterPhase = REGISTERING,

    total: Int = 0,  // init(total)로 설정
    info: (String, Int) = ("", 0), // (IP, PORT) of last registered worker

    registered: Map[String, WorkerInfo] = Map.empty, // IP → WorkerInfo (unique)
    sampled: Set[String] = Set.empty,                // IP only (unique)

    shutdown: Map[String, ShutdownState] = Map.empty
  ) {

    def registeringCurrent: Int = registered.size
    def samplingCurrent: Int = sampled.size

    def withTotal(n: Int): MasterState =
      copy(total = n)
    
    def withInfo(ip: String, port: Int): MasterState =
      copy(info = (ip, port))

    // ========== REGISTERING ==========
    def addRegistering(w: WorkerInfo): MasterState =
      copy(
        registered = registered + (w.ip -> w),  // IP 중복 제거
        shutdown = shutdown.updated(w.ip, NOT_YET)
      )

    // ========== SAMPLING ==========
    def addSampling(ip: String): MasterState =
      copy(sampled = sampled + ip)

    // ========== SHUTDOWN ==========
    def updateShutdownState(ip: String, s: ShutdownState): MasterState =
      copy(shutdown = shutdown.updated(ip, s))
  }

  // ============================================================
  // Global Manager (Mutable Singleton)
  // ============================================================
  object MasterManager {

    private var state: MasterState = MasterState()

    def getState: MasterState = state

    // 초기 worker count 설정
    def init(totalWorkers: Int, info: (String, Int)): Unit = synchronized {
      state = state.withTotal(totalWorkers).withInfo(info._1, info._2)
    }

    // REGISTERING 업데이트 (IP & PORT)
    def updateRegistering(ip: String, port: Int): Unit = synchronized {
      state = state.addRegistering(WorkerInfo(ip, port))
    }

    // SAMPLING 업데이트 (IP only)
    def updateSampling(ip: String): Unit = synchronized {
      state = state.addSampling(ip)
    }

    // SHUTDOWN 상태 업데이트
    def updateShutdown(ip: String, s: ShutdownState): Unit = synchronized {
      state = state.updateShutdownState(ip, s)
    }

    def updatePhase(p: MasterPhase): Unit = synchronized {
      state = state.copy(phase = p)
    }
  }

  // ============================================================
  // Progress Bar (Workers 기준)
  // ============================================================
  def progressBar(current: Int, total: Int, barWidth: Int = 20): String = {
    if (total == 0)
      return s"--% [${" " * barWidth}] $current / $total"

    val percent = (current.toDouble / total * 100).toInt
    val filled = (percent * barWidth) / 100
    val empty  = barWidth - filled

    val bar =
      "[" +
        "=" * filled +
        (if (filled < barWidth) ">" else "") +
        " " * (empty - 1).max(0) +
        "]"

    f"$percent%3d%% $bar $current / $total"
  }

  // ============================================================
  // Dashboard Builder (String) - IP 기준 정렬 반영
  // ============================================================
  def buildDashboard(st: MasterState): String = {

    val regLine  = s"REGISTERING: ${progressBar(st.registeringCurrent, st.total)}"
    val sampLine = s"SAMPLING:    ${progressBar(st.samplingCurrent, st.total)}"

    // WORKERS: IP 기준 정렬
    val workersBlock =
      if (st.registered.isEmpty)
        "| WORKERS:\n|"
      else {
        val lines =
          st.registered.values.toSeq
            .sortBy(_.ip) // 문자열 기준 정렬 (A 선택)
            .map(w => s"|   - ${w.ip}:${w.port}")
            .mkString("\n")
        s"| WORKERS:\n$lines"
      }

    // SHUTDOWN: IP 기준 정렬
    val shutdownBlock =
      if (st.shutdown.isEmpty)
        "| SHUTDOWN:\n|"
      else {
        val lines =
          st.shutdown.toSeq
            .sortBy(_._1) // ip 기준 정렬
            .map { case (ip, status) => s"|   - $ip: ${status.toString}" }
            .mkString("\n")
        s"| SHUTDOWN:\n$lines"
      }

    raw"""
======================================================================
| IP:Port: ${st.info._1}:${st.info._2}
| Status: ${st.phase}
|
| $regLine
| $sampLine
|
$workersBlock
|
$shutdownBlock
|
======================================================================
""".stripMargin
  }

  // ============================================================
  // Console Control
  // ============================================================
  object ConsoleCtrl {
    def moveUp(n: Int) = s"\u001b[${n}A"
    val clearLine = "\u001b[2K"
  }

  // ============================================================
  // Dashboard Renderer (자동 갱신)
  // ============================================================
  object DashboardRenderer {

    import ConsoleCtrl._

    private var printedLines = 0

    def renderOnce(): Unit = {
      val text = buildDashboard(MasterManager.getState)
      val lines = text.count(_ == '\n')

      // 이전 출력 지우기
      if (printedLines > 0) {
        print(moveUp(printedLines))
        for (_ <- 0 until printedLines) {
          print(clearLine + "\n")
        }
        print(moveUp(printedLines))
      }

      // 새 출력
      print(text)
      printedLines = lines
    }

    // 0.3초마다 자동 렌더링
    def start(intervalMs: Int = 300): Unit = {
      new Thread(() => {
        while (true) {
          renderOnce()
          Thread.sleep(intervalMs)
        }
      }).start()
    }
  }
}


object ExampleMaster {
  import MasterDashboard._
  def main(args: Array[String]): Unit = {

    DashboardRenderer.start()

    MasterManager.init(3, ("0.0.0.0", 0))
    Thread.sleep(500)

    MasterManager.updateRegistering("1.1.1.1", 7777)
    Thread.sleep(500)
    MasterManager.updateRegistering("3.3.3.3", 7777)
    Thread.sleep(500)
    MasterManager.updateRegistering("2.2.2.2", 8888)

    // IP 정렬 잘 되는지 확인용
    MasterManager.updateSampling("1.1.1.1")
    MasterManager.updateSampling("2.2.2.2")
    MasterManager.updateShutdown("2.2.2.2", SHUTDOWN_RECEIVED)

    Thread.sleep(5000)
  }
}
