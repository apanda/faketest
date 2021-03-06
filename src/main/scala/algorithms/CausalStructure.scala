package akka.dispatch.verification.algorithm 

import akka.dispatch.verification._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList, HashMap}
import scala.util.control.Breaks._

case class CausalGraph (schedule: Array[Event], // The schedule we originally analyzed
                        ctxStepForEvent: HashMap[Int, Int], // Context time step for event
                        enabledAtCtxStep: HashMap[Int, Int], // What context step something became enabled
                        enabledAtSchedStep: HashMap[Int, Int], // What schedule step something became enabled
                        actorForCtxStep: HashMap[Int, String], // Who is running in a context
                        causalDependency: HashMap[Int, Queue[Int]], // Who came before an event causally
                        messageDependency: HashMap[Int, Int]) // What message (if any), came before.
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
    val messageDependency = new HashMap[Int, Int]

    val messageSendEvents = new HashMap[MsgSend, Queue[Int]]
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

        case m: MsgSend =>
          messageSendEvents(m) = 
          messageSendEvents.getOrElse(m, new Queue[Int]) ++ List(evIdx)

        case SpawnEvent(_, _, actor, _) =>
          lastStepByActor(actor) = evIdx

        case MsgEvent(sender, receiver, msg) =>
          // Assume consuming in order of sends, this holds for Akka and anything using TCP
          val msgSendObj = MsgSend(sender, receiver, msg)
          val msgSendIdx = messageSendEvents(msgSendObj).dequeue()
          enabledAtSchedStep(evIdx) = msgSendIdx
          enabledAtCtxStep(evIdx) = ctxStepForEvent(msgSendIdx)
          evCausalDependencies += (msgSendIdx)

        case PartitionEvent((actorA, actorB)) =>
          // The subsequent behavior of the actors is in some sense causally dependent on
          // the partition happening.
          lastStepByActor(actorA) = evIdx
          lastStepByActor(actorB) = evIdx
          // The partition itself of course is not, it just happens.

        case UnPartitionEvent((actorA, actorB)) =>
          // The subsequent behavior of the actors is in some sense causally dependent on
          // whether the partition being fixed.
          lastStepByActor(actorA) = evIdx
          lastStepByActor(actorB) = evIdx

        case Quiescence => 
          // TODO: All subsequent external events are somewhat dependent on this (it says where they should be
          // allowed to happen), but in reality, it matters in strange ways: changing quiescence might 
          // affect everything in some strange ways. 
          ()
        case _ => ()
      }

      lastStepByActor(context) = evIdx
      ctxStepForEvent(evIdx) = ctxStep 
      causalDependency(evIdx) = evCausalDependencies
    }
    CausalGraph(schedule, 
                ctxStepForEvent, 
                enabledAtCtxStep, 
                enabledAtSchedStep, 
                actorForCtxStep, 
                causalDependency,
                messageDependency)
  }
  
  def causalChain (causality: CausalGraph, idx: Int): Queue[Int] = {
    val causalChain = new Queue[Int]
    val causalSet = new HashSet[Int]

    causalChain += idx
    causalSet += idx

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

  // Given a causal graph, a set of external events and an index find all causally related events for the external 
  // event.
  def causalChain (causality: CausalGraph, external: Array[ExternalEvent], idx: Int) : Queue[Int] = {
    val extToInt = Utilities.alignSchedules(external, causality.schedule) 
    val startingPoint = extToInt(idx)
    causalChain(causality, startingPoint)
  }

  private[this] def isRacing (idxA: Int, idxB: Int, chainA: Queue[Int], chainB: Queue[Int]) : Boolean = {
    !((chainB contains idxA) || (chainA contains idxB))
  }

  // Are two events racing, i.e., such that neither happens before the other.
  // TODO: One can do this in constant time with Lamport clocks, however this seems to track more than what we want at
  // this point. So, think about this more.
  def isRacing (causality: CausalGraph, idxA: Int, idxB: Int) : Boolean = {
    val chainA = causalChain(causality, idxA)
    val chainB = causalChain(causality, idxB)
    isRacing(idxA, idxB, chainA, chainB)
  }


  // Find all pairs of racing events. Given all racing events we can reverse them when needed, etc. to actually get
  // random choices.
  def allRacing (causality: CausalGraph, idxA: Int) : Queue[(Int, Int)] = {
    val queue = new Queue[(Int, Int)]
    val all_causal = new HashMap[Int, Queue[Int]] ++ 
                       (0 until causality.schedule.length).map(x => (x, causalChain(causality, x)))
    for (idxB <- idxA + 1 until causality.schedule.length) {
      if (isRacing(idxA, idxB, all_causal(idxA), all_causal(idxB))) {
        queue += ((idxA, idxB))
      }
    }
    queue
  }

}
