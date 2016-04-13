package nasa.nccs.cdapi.cdm

import com.vividsolutions.jts.geom
import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.ShapefileReader
import ucar.ma2

class GeoTools( val SRID: Int = 0 ) {
  val precisionModel = new geom.PrecisionModel( geom.PrecisionModel.FLOATING_SINGLE )
  val geometryFactory = new geom.GeometryFactory( precisionModel, SRID)

  def readShapefile( filePath: String ): List[geom.MultiPolygon] = {
    val in = new ShpFiles( filePath )
    val r: ShapefileReader = new ShapefileReader( in, false, false, geometryFactory  )
    val buf = scala.collection.mutable.ListBuffer.empty[geom.MultiPolygon]
    while (r.hasNext()) {
      buf.append( r.nextRecord().shape().asInstanceOf[geom.MultiPolygon] )
    }
    r.close()
    buf.toList
  }

  def getMask( boundary: geom.MultiPolygon, bounds: Array[Float], shape: Array[Int], SRID: Int = 0 ): Array[Byte] = {
    val dx = (bounds(1)-bounds(0))/shape(0)
    val dy = (bounds(3)-bounds(2))/shape(1)
    val mask = for( ix <- (0 until shape(0)); iy <- (0 until shape(1)); x = bounds(0) + ix*dx; y = bounds(1) + iy*dy )
      yield ( if(boundary.contains( geometryFactory.createPoint(new geom.Coordinate(x,y)))) 1 else 0 ).toByte
    mask.toArray
  }

}


object readShapefileTest extends App {
  val oceanShapeUrl=getClass.getResource("/shapes/ocean/ne_50m_ocean.shp")
  val geotools = new GeoTools()
  val shapes: List[geom.MultiPolygon] = geotools.readShapefile( oceanShapeUrl.getPath() )
  val mask = geotools.getMask( shapes.head, Array(0f,360f,-180f,180f), Array(360,180) )
  println( shapes.length )
}

