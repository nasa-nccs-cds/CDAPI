package nccs.engine
import nccs.process.TaskRequest
import org.slf4j.LoggerFactory

abstract class ExecutionManager {
  val logger = LoggerFactory.getLogger( classOf[ExecutionManager] )

  def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem
}

object demoExecutionManager extends ExecutionManager {

  override def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem = {
    val request = TaskRequest( process_name, datainputs )
    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
    request.toXml()
  }
}
