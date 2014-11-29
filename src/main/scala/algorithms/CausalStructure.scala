package akka.dispatch.verification.algorithm 

import akka.dispatch.verification._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList, HashMap}
import scala.util.control.Breaks._

case class CausalGraph (schedule: Array[Event], // The schedule we originally analyzed
                        ctxStepForEvent: HashMap[Int, Int], // Context time step for event
                        enabledAtCtxStep: HashMap[Int, Int], // What context step something became enabled
                        enabledAtSchedStep: HashMap[Int, Int], // What schedule step something became enabled
                        actorForCtxStep: HashMap[Int, String],
                        causalDependency: HashMap[Int, Queue[Int]]) // Who is running in a context
object CausalStructure {
  // Given a schedule produce a causal graph for all the events in the schedule.
  def computeCausalGraph (schedule:Array[Event]) : CausalGraph = {
    var ctxStep = 0
    var context = "scheduler"
    val ctxStepForEvent = new HashMap[Int, Int]
    val enabledAtCtxStep = new HashMap[Int, Int]
    val enabledAtSchedStep = new HashMap[Int, Int]
    val actorForCtxStep = new HashMap[Int, String]
    val causalDependency = new HashMap[Int, Queue[Int]]

    val messageSendEvents = new HashMap[(String, String, Any), Queue[Int]]
    val lastStepByActor = new HashMap[String, Int]
    
    actorForCtxStep(ctxStep) = context
    for (evIdx <- 0 until schedule.length) {
      val ev = schedule(evIdx)
      val evCausalDependencies = new Queue[Int]
      lastStepByActor.get(context) match {
        case Some(prev) =>
          evCausalDependencies += (prev)
        case _ => ()
      }
      
      ev match {
        case ChangeContext(actor) =>
          ctxStep += 1
          context = actor
          actorForCtxStep(ctxStep) = context
          evCausalDependencies.clear()
        case MsgSend(sender, receiver, msg) =>
          messageSendEvents((sender, receiver, msg)) = 
          messageSendEvents.getOrElse((sender, receiver, msg), new Queue[Int]) ++ List(evIdx)
        case SpawnEvent(_, _, actor, _) =>
          lastStepByActor(actor) = evIdx
        case MsgEvent(sender, receiver, msg) =>
          // Assume consuming in order of sends, this holds for Akka and anything using TCP
          val msgSendIdx = messageSendEvents((sender, receiver, msg)).dequeue()
          enabledAtSchedStep(evIdx) = msgSendIdx
          enabledAtCtxStep(evIdx) = ctxStepForEvent(msgSendIdx)
          evCausalDependencies += (msgSendIdx)
        case _ => ()
      }

      lastStepByActor(context) = evIdx
      ctxStepForEvent(evIdx) = ctxStep 
      causalDependency(evIdx) = evCausalDependencies
    }
    CausalGraph(schedule, ctxStepForEvent, enabledAtCtxStep, enabledAtSchedStep, actorForCtxStep, causalDependency)
  }
  
  // Given a causal graph, a set of external events and an index find all causally related events for the external 
  // event.
  def causalChain (causality: CausalGraph, external: Array[ExternalEvent], idx: Int) : Queue[Int] = {
    val causalChain = new Queue[Int]
    val extToInt = Utilities.alignSchedules(external, causality.schedule) 
    val startingPoint = extToInt(idx)
    val causalSet = new HashSet[Int]

    causalChain += startingPoint
    causalSet += startingPoint

    for (evIdx <- 0 until causality.schedule.length) {
      for (deps <- causality.causalDependency(evIdx)) {
        if (causalSet contains deps) {
          causalChain += evIdx
          causalSet += evIdx
        }
      }
    }
    causalChain
  }
}
