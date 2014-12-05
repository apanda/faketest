import akka.actor._
import akka.dispatch.Dispatcher
import akka.dispatch.verification._
import scala.concurrent.duration._
import akka.dispatch.verification.algorithm._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList}
import scala.collection.immutable.HashMap
import scala.collection.MapLike
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.annotation.tailrec

// Failure detector messages (the FD is perfect in this case)
final case class Killed (name: String) {}
final case class Started (name: String) {}
final case class GroupMembership (members: Iterable[String]) {}

object Test extends App {
  def verifyState (actors: Array[String], 
                   states: Map[String, BCastState]) : Boolean = {
    var same = true
    for (act <- actors) {
      for (act2 <- actors) {
        if (states(act).messages != states(act2).messages) {
          println(act + "  " + act2 + " differ")
          println(states(act).messages + "   " + states(act2).messages)
          same = false
        }
      }
    }
    same
  }
  val actors = Array("bcast1",
                     "bcast2",
                     "bcast3",
                     "bcast4",
                     "bcast5",
                     "bcast6",
                     "bcast7",
                     "bcast8")
  val state = HashMap[String, BCastState]() ++
              actors.map((_, new BCastState()))

  val trace0 = Array[ExternalEvent]() ++
    actors.map(
      act => Start(Props.create(classOf[ReliableBCast],
            state(act)), act)) ++
    //actors.map(Send(_, GroupMembership(actors))) ++ 
    Array[ExternalEvent](
    WaitQuiescence,
    Partition("bcast8", "bcast1"),
    Send("bcast5", Bcast(null, Msg("Foo", 1))),
    Send("bcast8", Bcast(null, Msg("Bar", 2))),
    Partition("bcast8", "bcast2"),
    Partition("bcast8", "bcast3"),
    Partition("bcast8", "bcast4"),
    Partition("bcast8", "bcast5"),
    Partition("bcast8", "bcast6"),
    Partition("bcast8", "bcast7")
  )

  val state1 = HashMap[String, BCastState]() ++
              actors.map((_, new BCastState()))

  val actorProps = HashMap[String, Props]() ++
                   actors.map(name => (name, Props.create(classOf[ReliableBCast],
            state(name))))

  val trace1 = Array[ExternalEvent]() ++
    actors.map(
      act => Start(Props.create(classOf[ReliableBCast],
            state1(act)), act)) ++
    //actors.map(Send(_, GroupMembership(actors))) ++ 
    Array[ExternalEvent](
    WaitQuiescence,
    Partition("bcast8", "bcast1"),
    Send("bcast5", Bcast(null, Msg("Foo", 1))),
    Partition("bcast8", "bcast2"),
    Partition("bcast8", "bcast3"),
    Partition("bcast8", "bcast4"),
    Partition("bcast8", "bcast5"),
    Partition("bcast8", "bcast6"),
    Partition("bcast8", "bcast7")
  )
  val removed = Array() ++
                Array(trace0.indexOf(Send("bcast8", Bcast(null, Msg("Bar", 2)))))
                //Array(trace0.indexOf(Send("bcast5", Bcast(null, Msg("Foo", 1)))))

  val sched = new PeekScheduler
  Instrumenter().scheduler = sched
  val events = sched.peek(trace0)
  println("Returned to main with events")
  println("Shutting down")
  sched.shutdown
  println("Shutdown successful")

  verifyState(actors, state) 

  for (evIdx <- 0 until events.length) {
    println(evIdx + "  " + events(evIdx))
  }
  //val struct = CausalStructure.computeCausalGraph(events.toArray)
  //val removedChain = CausalStructure.causalChain(struct, trace0, removed(0))
  //println("================================================================================================")
  //for (evIdx <- removedChain) {
    //val dependency = struct.enabledAtSchedStep.get(evIdx) match {
      //case Some(idx) =>
        //idx + " " + struct.schedule(idx).toString
      //case None =>
        //""
    //}
    //println(evIdx + "   " + 
            //events(evIdx) + "   " + 
            //struct.ctxStepForEvent(evIdx) + "  " + 
            //struct.actorForCtxStep(struct.ctxStepForEvent(evIdx)) + "   " +
            //dependency)
    //for (depIdx <- struct.causalDependency(evIdx)) {
      //println("      " + 
              //depIdx + "   " +
              //struct.actorForCtxStep(struct.ctxStepForEvent(depIdx)) + "   " +
              //struct.schedule(depIdx))
    //}
  //}

  //println("Check racing " + CausalStructure.isRacing(struct, removedChain(1), removedChain(2)))
  //val leftIdxes = ((0 until events.length).toSet -- removedChain).toArray
  //println("Check racing " + CausalStructure.isRacing(struct, leftIdxes(1), removedChain(2)))
  //println("================================================================================================")
  //println("Start computing racing")
  //val pairs = CausalStructure.allRacing(struct, 551) 
  //println("Stop computing racing")
  //for ((evA, evB) <- pairs) {
    //println(evA + "    " + struct.actorForCtxStep(struct.ctxStepForEvent(evA)) + "    " +  
            //events(evA) + "  RACING  " +  evB + "    " + struct.actorForCtxStep(struct.ctxStepForEvent(evA)) + "    " + 
             //events(evB))
  //}
}
