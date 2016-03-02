package nasa.nccs.esgf.process

import scala.util.matching.Regex
import scala.collection.{mutable, immutable}
import scala.collection.mutable.HashSet
import scala.xml._
import mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import nasa.nccs.esgf.utilities.wpsNameMatchers

case class ErrorReport(severity: String, message: String) {
  override def toString = {
    s"ErrorReport { severity: $severity, message: $message }"
  }

  def toXml = {
      <error severity={severity} message={message}/>
  }
}

class TaskRequest(val name: String, val variableMap : Map[String,DataContainer], val domainMap: Map[String,DomainContainer], val workflows: List[WorkflowContainer] = List() ) {
  val errorReports = new ListBuffer[ErrorReport]()
  val logger = LoggerFactory.getLogger( this.getClass )
  validate()
//  logger.info( s"TaskRequest: name= $name, workflows= " + workflows.toString + ", variableMap= " + variableMap.toString + ", domainMap= " + domainMap.toString )

  def addErrorReport(severity: String, message: String) = {
    val error_rep = ErrorReport(severity, message)
    logger.error(error_rep.toString)
    errorReports += error_rep
  }

  def getDomain( data_source: DataSource ): DomainContainer= {
    domainMap.get(data_source.domain) match {
      case Some(domain_container) => domain_container
      case None =>
        throw new Exception("Undefined domain for dataset " + data_source.name + ", domain = " + data_source.domain)
    }
  }

  def validate() = {
    for( variable <- inputVariables; if variable.isSource; domid = variable.getSource.domain; vid=variable.getSource.name; if !domid.isEmpty ) {
      if ( !domainMap.contains(domid) ) {
        var keylist = domainMap.keys.mkString("[",",","]")
        logger.error( s"Error, No $domid in $keylist in variable $vid" )
        throw new Exception( s"Error, Missing domain $domid in variable $vid" )
      }
    }
    for (workflow <- workflows; operation <- workflow.operations; opid = operation.name; var_arg <- operation.inputs) {
      if (!variableMap.contains(var_arg)) {
        var keylist = variableMap.keys.mkString("[", ",", "]")
        logger.error(s"Error, No $var_arg in $keylist in operation $opid")
        throw new Exception(s"Error, Missing variable $var_arg in operation $opid")
      }
    }
  }

  override def toString = {
    var taskStr = s"TaskRequest { name='$name', variables = '$variableMap', domains='$domainMap', workflows='$workflows' }"
    if ( errorReports.nonEmpty ) {
      taskStr += errorReports.mkString("\nError Reports: {\n\t", "\n\t", "\n}")
    }
    taskStr
  }

  def toXml = {
    <task_request name={name}>
      <data>
        { inputVariables.map(_.toXml )  }
      </data>
      <domains>
        {domainMap.values.map(_.toXml ) }
      </domains>
      <operation>
        { workflows.map(_.toXml ) }
      </operation>
      <error_reports>
        {errorReports.map(_.toXml ) }
      </error_reports>
    </task_request>
  }

  def inputVariables: Traversable[DataContainer] = {
    for( variableSource <- variableMap.values; if variableSource.isInstanceOf[ DataContainer ] ) yield variableSource.asInstanceOf[DataContainer]
  }
}

object TaskRequest {
  val logger = LoggerFactory.getLogger( this.getClass )
  def apply(process_name: String, datainputs: Map[String, Seq[Map[String, Any]]]) = {
    logger.info( "TaskRequest--> process_name: %s, datainputs: %s".format( process_name, datainputs.toString ) )
    val data_list = datainputs.getOrElse("variable", List()).map(DataContainer(_)).toList
    val domain_list = datainputs.getOrElse("domain", List()).map(DomainContainer(_)).toList
    val operation_list = datainputs.getOrElse("operation", List()).map(WorkflowContainer(process_name,_)).toList
    val variableMap = buildVarMap( data_list, operation_list )
    val domainMap = buildDomainMap( domain_list )
    new TaskRequest( process_name, variableMap, domainMap, operation_list )
  }

  def buildVarMap( data: List[DataContainer], workflow: List[WorkflowContainer] ): Map[String,DataContainer] = {
    var var_items = new ListBuffer[(String,DataContainer)]()
    for( data_container <- data ) var_items += ( data_container.uid -> data_container )
    for( workflow_container<- workflow; operation<-workflow_container.operations; if !operation.result.isEmpty ) var_items += ( operation.result -> DataContainer(operation) )
    val var_map = var_items.toMap[String,DataContainer]
    logger.info( "Created Variable Map: " + var_map.toString )
    for( workflow_container<- workflow; operation<-workflow_container.operations; vid<-operation.inputs  ) var_map.get( vid ) match {
      case Some(data_container) => data_container.addOpSpec( operation )
      case None => throw new Exception( "Unrecognized variable %s in varlist [%s]".format( vid, var_map.keys.mkString(",") ) )
    }
    var_map
  }

  def buildDomainMap( domain: List[DomainContainer] ): Map[String,DomainContainer] = {
    var domain_items = new ListBuffer[(String,DomainContainer)]()
    for( domain_container <- domain ) domain_items += ( domain_container.name -> domain_container )
    val domain_map = domain_items.toMap[String,DomainContainer]
    logger.info( "Created Domain Map: " + domain_map.toString )
    domain_map
  }
}

class ContainerBase {
  val logger = LoggerFactory.getLogger( this.getClass )
  def item_key(map_item: (String, Any)): String = map_item._1

  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase

  def getStringKeyMap( generic_map: Map[_,_] ): Map[String,Any] = {
    assert( generic_map.isEmpty | generic_map.keys.head.isInstanceOf[ String ] )
    generic_map.asInstanceOf[ Map[String,Any] ]
  }

  def key_equals(key_value: String)(map_item: (String, Any)): Boolean = {
    item_key(map_item) == key_value
  }

  def key_equals(key_regex: Regex)(map_item: (String, Any)): Boolean = {
    key_regex.findFirstIn(item_key(map_item)) match {
      case Some(x) => true;
      case None => false;
    }
  }

  //  def key_equals( key_expr: Iterable[Any] )( map_item: (String, Any) ): Boolean = { key_expr.map( key_equals(_)(map_item) ).find( ((x:Boolean) => x) ) }
  def filterMap(raw_metadata: Map[String, Any], key_matcher: (((String, Any)) => Boolean)): Any = {
    raw_metadata.find(key_matcher) match {
      case Some(x) => x._2;
      case None => None
    }
  }

  def toXml = {
    <container>
      {"<![CDATA[ " + toString + " ]]>"}
    </container>
  }

  def getGenericNumber( opt_val: Option[Any] ): GenericNumber = {
    opt_val match {
      case Some(p) => GenericNumber(p)
      case None =>    GenericNumber()
    }
  }
  def getStringValue( opt_val: Option[Any] ): String = {
    opt_val match {
      case Some(p) => p.toString
      case None => ""
    }
  }
}

object containerTest extends App {
  val c = new ContainerBase()
  val tval = Some( 4.7 )
  val fv = c.getGenericNumber( tval )
  println( fv )
}

class DataSource( val name: String, val collection: String, val domain: String ) {
  override def toString =  s"DataSource { name = $name, collection = $collection, domain = $domain }"
  def toXml = <dataset name={name} collection={collection.toString} domain={domain.toString}/>
}

object OperationSpecs {
  def apply( op: OperationContainer ) = new OperationSpecs( op.name, op.optargs )
}
class OperationSpecs( id: String, val optargs: Map[String,String] ) {
  val ids = mutable.HashSet( id )
  def ==( oSpec: OperationSpecs ) = ( optargs == oSpec.optargs )
  def merge ( oSpec: OperationSpecs  ) = { ids ++= oSpec.ids }
  def getSpec( id: String, default: String = "" ): String = optargs.getOrElse( id, default )
}


class DataContainer(val uid: String, private val source : Option[DataSource] = None, private val operation : Option[OperationContainer] = None ) extends ContainerBase {
  assert( source.isDefined || operation.isDefined, "Empty DataContainer: variable uid = $uid" )
  assert( source.isEmpty || operation.isEmpty, "Conflicted DataContainer: variable uid = $uid" )
  private val optSpecs = mutable.ListBuffer[ OperationSpecs ]()

  override def toString = {
    val embedded_val: String = if ( source.isDefined ) source.get.toString else operation.get.toString
    s"DataContainer ( $uid ) { $embedded_val }"
  }
  override def toXml = {
    val embedded_xml = if ( source.isDefined ) source.get.toXml else operation.get.toXml
    <dataset uid={uid}> embedded_xml </dataset>
  }
  def isSource = source.isDefined
  def isOperation = operation.isDefined
  def getSource = {
    assert( isSource, s"Attempt to access an operation based DataContainer($uid) as a data source")
    source.get
  }
  def getOperation = {
    assert( isOperation, s"Attempt to access a source based DataContainer($uid) as an operation")
    operation.get
  }

  def addOpSpec( operation: OperationContainer ): Unit = {
    def mergeOpSpec( oSpecList: mutable.ListBuffer[ OperationSpecs ], oSpec: OperationSpecs ): Unit = oSpecList.headOption match {
      case None => oSpecList += oSpec
      case Some(head) => if( head == oSpec ) head merge oSpec else mergeOpSpec(oSpecList.tail,oSpec)
    }
    mergeOpSpec( optSpecs, OperationSpecs(operation) )
  }
  def getOpSpecs: List[OperationSpecs] = optSpecs.toList
}

object DataContainer extends ContainerBase {
  def apply( operation: OperationContainer ): DataContainer = {
      new DataContainer( uid=operation.result, operation=Some(operation) )
  }
  def apply(metadata: Map[String, Any]): DataContainer = {
    try {
      val uri = filterMap(metadata, key_equals("uri"))
      val fullname = filterMap(metadata, key_equals("name"))
      val domain = filterMap(metadata, key_equals("domain"))
      val name_items = fullname.toString.split(':')
      val collection = parseUri( uri.toString )
      val dsource = new DataSource( normalize(name_items.head), normalize(collection), normalize(domain.toString) )
      new DataContainer(normalize(name_items.last), source = Some(dsource) )
    } catch {
      case e: Exception =>
        logger.error("Error creating DataContainer: " + e.getMessage  )
        logger.error( e.getStackTrace.mkString("\n") )
        throw new Exception( e.getMessage, e )
    }
  }
  def parseUri( uri: String ): String = {
    val uri_parts = uri.split("://")
    val url_type = normalize( uri_parts.head )
    if( ( url_type == "collection" ) && ( uri_parts.length == 2 ) ) uri_parts.last
    else throw new Exception( "Unrecognized uri format: " + uri + ", type = " + uri_parts.head + ", nparts = " + uri_parts.length.toString + ", value = " + uri_parts.last )
  }
}

class DomainContainer( val name: String, val axes: List[DomainAxis] ) extends ContainerBase {
  override def toString = {
    s"DomainContainer { name = $name, axes = $axes }"
  }
  override def toXml = {
    <domain name={name}>
      <axes> { axes.map( _.toXml ) } </axes>
    </domain>
  }
}

object DomainAxis extends ContainerBase {
  object Type extends Enumeration { val Lat, Lon, Lev, X, Y, Z, T = Value }

  def apply( axistype: Type.Value, start: Int, end: Int ): Option[DomainAxis] = {
    Some( new DomainAxis(  axistype, start, end, "indices" ) )
  }

  def apply( axistype: Type.Value, axis_spec: Any ): Option[DomainAxis] = {
    axis_spec match {
      case generic_axis_map: Map[_,_] =>
        val axis_map = getStringKeyMap( generic_axis_map )
        val start = getGenericNumber( axis_map.get("start") )
        val end = getGenericNumber( axis_map.get("end") )
        val system = getStringValue( axis_map.get("system") )
        val bounds = getStringValue( axis_map.get("bounds") )
        Some( new DomainAxis( axistype, start, end, normalize(system), normalize(bounds) ) )
      case None => None
      case _ =>
        val msg = "Unrecognized DomainAxis spec: " + axis_spec.getClass.toString
        logger.error( msg )
        throw new Exception(msg)
    }
  }
}

class DomainAxis( val axistype: DomainAxis.Type.Value, val start: GenericNumber, val end: GenericNumber, val system: String, val bounds: String = "" ) extends ContainerBase  {
  val name =   axistype.toString

  override def toString = {
    s"DomainAxis { name = $name, start = $start, end = $end, system = $system, bounds = $bounds }"
  }

  override def toXml = {
    <axis name={name} start={start.toString} end={end.toString} system={system} bounds={bounds} />
  }
}

object DomainContainer extends ContainerBase {
  
  def apply(metadata: Map[String, Any]): DomainContainer = {
    var items = new ListBuffer[ Option[DomainAxis] ]()
    try {
      val name = filterMap(metadata, key_equals("name"))
      items += DomainAxis( DomainAxis.Type.Lat, filterMap(metadata,  key_equals( wpsNameMatchers.latAxis )))
      items += DomainAxis( DomainAxis.Type.Lon, filterMap(metadata,  key_equals( wpsNameMatchers.lonAxis )))
      items += DomainAxis( DomainAxis.Type.Lev, filterMap(metadata,  key_equals( wpsNameMatchers.levAxis )))
      items += DomainAxis( DomainAxis.Type.Y,   filterMap(metadata,  key_equals( wpsNameMatchers.yAxis )))
      items += DomainAxis( DomainAxis.Type.X,   filterMap(metadata,  key_equals( wpsNameMatchers.xAxis )))
      items += DomainAxis( DomainAxis.Type.Z,   filterMap(metadata,  key_equals( wpsNameMatchers.zAxis )))
      items += DomainAxis( DomainAxis.Type.T,   filterMap(metadata,  key_equals( wpsNameMatchers.tAxis )))
      new DomainContainer( normalize(name.toString), items.flatten.toList )
    } catch {
      case e: Exception =>
        logger.error("Error creating DomainContainer: " + e.getMessage )
        logger.error( e.getStackTrace.mkString("\n") )
        throw new Exception( e.getMessage, e )
    }
  }
}

class WorkflowContainer(val operations: Iterable[OperationContainer] = List() ) extends ContainerBase {
  override def toString = {
    s"WorkflowContainer { operations = $operations }"
  }
  override def toXml = {
    <workflow>  { operations.map( _.toXml ) }  </workflow>
  }
}

object WorkflowContainer extends ContainerBase {
  def apply(process_name: String, metadata: Map[String, Any]): WorkflowContainer = {
    try {
      import nasa.nccs.esgf.utilities.wpsOperationParser
      val parsed_data_inputs = wpsOperationParser.parseOp(metadata("unparsed").toString)
      new WorkflowContainer( parsed_data_inputs.map(OperationContainer(process_name,_)))
    } catch {
      case e: Exception =>
        val msg = "Error creating WorkflowContainer: " + e.getMessage
        logger.error(msg)
        throw new Exception(msg)
    }
  }
}

class OperationContainer(val identifier: String, val name: String, val result: String = "", val inputs: List[String], val optargs: Map[String,String])  extends ContainerBase {
  override def toString = {
    s"OperationContainer { id = $identifier,  name = $name, result = $result, inputs = $inputs, optargs = $optargs }"
  }
  override def toXml = {
    <proc id={identifier} name={name} result={result} inputs={inputs.toString} optargs={optargs.toString}/>
  }
}

object OperationContainer extends ContainerBase {
  def apply(process_name: String, raw_metadata: Any): OperationContainer = {
    raw_metadata match {
      case (ident: String, args: List[_]) =>
        val varlist = new ListBuffer[String]()
        val optargs = new ListBuffer[(String,String)]()
        for( raw_arg<-args; arg=raw_arg.toString ) {
          if(arg contains ":") {
            val arg_items = arg.split(":").map( _.trim.toLowerCase )
            optargs += ( arg_items(0) -> arg_items(1) )
          }
          else varlist += arg.trim.toLowerCase
        }
        val ids = ident.split("~").map( _.trim.toLowerCase )
        ids.length match {
          case 1 => new OperationContainer( identifier = ident, name=process_name, result = ids(0), inputs = varlist.toList, optargs=optargs.toMap[String,String] )
          case 2 =>
            val op_name = if( ids(0).nonEmpty ) ids(0) else process_name
            val identifier = if( ids(0).nonEmpty ) ident else process_name + ident
            new OperationContainer( identifier = identifier, name = op_name, result = ids(1), inputs = varlist.toList, optargs=optargs.toMap[String,String] )
          case _ =>
            val msg = "Unrecognized format for Operation id: " + ident
            logger.error(msg)
            throw new Exception(msg)
        }
      case _ =>
        val msg = "Unrecognized format for OperationContainer: " + raw_metadata.toString
        logger.error(msg)
        throw new Exception(msg)
    }
  }
}


class TaskProcessor {

}

object enumTest extends App {
  object Type extends Enumeration { val Lat, Lon, Lev, X, Y, Z, T = Value }

  val strType = Type.Lat.toString
  println( strType )

}
