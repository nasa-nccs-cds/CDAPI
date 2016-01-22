package nccs.esgf.engine
import nccs.esgf.process.TaskRequest
import org.slf4j.LoggerFactory

abstract class PluginExecutionManager {
  val logger = LoggerFactory.getLogger( classOf[PluginExecutionManager] )

  def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem
}

object demoExecutionManager extends PluginExecutionManager {

  override def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem = {
    val request = TaskRequest( process_name, datainputs )
    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
    request.toXml
  }
}
