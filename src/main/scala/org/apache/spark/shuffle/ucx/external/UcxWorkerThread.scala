package org.apache.spark.shuffle.ucx

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentLinkedQueue
import org.openucx.jucx.ucp.UcpWorker

class UcxWorkerThread(worker: UcpWorker, useWakeup: Boolean) extends Thread {
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]
  private val waiting = new AtomicBoolean(false)
  private val running = new AtomicBoolean(true)

  setDaemon(true)
  setName(s"UCX-worker-${super.getName}")

  @`inline`
  def post(task: Runnable): Unit = {
    taskQueue.add(task)
    if (waiting.compareAndSet(true, false)) {
      worker.signal()
    }
  }

  @`inline`
  def await() = {
    if (taskQueue.isEmpty && waiting.compareAndSet(false, true) && taskQueue.isEmpty) {
      worker.waitForEvents()
    }
  }

  def close(): Unit = {
    if (running.compareAndSet(true, false)) {
      worker.close()
    }
  }

  override def run(): Unit = {
    val doAwait = if (useWakeup) await _ else () => {}
    while (running.get()) {
      val task = taskQueue.poll()
      if (task != null) {
        task.run()
      }
      while (worker.progress != 0) {}
      doAwait()
    }
  }
}