package akka.dispatch.verification.algorithm 

import akka.dispatch.verification._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList, HashMap}
import scala.util.control.Breaks._

case class CausalGraph (schedule: Array[Event], // The schedule we originally analyzed
                        ctxStepForEvent: HashMap[Int, Int], // Context time step for event
                        enabledAtCtxStep: HashMap[Int, Int], // What context step something became enabled
                        enabledAtSchedStep: HashMap[Int, Int], // What schedule step something became enabled
                        actorForCtxStep: HashMap[Int, String]) // Who is running in a context
object CausalStructure {
  def computeCausalGraph (schedule:Array[Event]) : CausalGraph = {
    var ctxStep = 0
    var context = "scheduler"
    val ctxStepForEvent = new HashMap[Int, Int]
    val enabledAtCtxStep = new HashMap[Int, Int]
    val enabledAtSchedStep = new HashMap[Int, Int]
    val actorForCtxStep = new HashMap[Int, String]
    val messageSendEvents = new HashMap[(String, String, Any), Queue[Int]]
    actorForCtxStep(ctxStep) = context
    for (evIdx <- 0 until schedule.length) {
      val ev = schedule(evIdx)
      ev match {
        case ChangeContext(actor) =>
          ctxStep += 1
          context = actor
          actorForCtxStep(ctxStep) = context
        case MsgSend(sender, receiver, msg) =>
          messageSendEvents((sender, receiver, msg)) = 
          messageSendEvents.getOrElse((sender, receiver, msg), new Queue[Int]) ++ List(evIdx)
          //enabledAtSchedStep(evIdx) = evIdx // Message sends are enabled by a send
          //enabledAtCtxStep(evIdx) = ctxStep
        case MsgEvent(sender, receiver, msg) =>
          // Assume consuming in order of sends, this holds for Akka and anything using TCP
          val msgSendIdx = messageSendEvents((sender, receiver, msg)).dequeue()
          enabledAtSchedStep(evIdx) = msgSendIdx
          enabledAtCtxStep(evIdx) = ctxStepForEvent(msgSendIdx)
        case _ => ()
      }
      ctxStepForEvent(evIdx) = ctxStep 
      
    }
    CausalGraph(schedule, ctxStepForEvent, enabledAtCtxStep, enabledAtSchedStep, actorForCtxStep)
  }
}
