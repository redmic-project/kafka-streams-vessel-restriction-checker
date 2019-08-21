package es.redmic.vesselrestrictionchecker.utils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

public class GeoUtils {

	private static final int MAX_LEVELS = 4;

	private static SpatialContext ctx = SpatialContext.GEO;

	private static WKTReader reader = new WKTReader(JtsSpatialContext.GEO, new JtsSpatialContextFactory());

	private static JtsWKTWriter writer = new JtsWKTWriter(JtsSpatialContext.GEO, new JtsSpatialContextFactory());

	private static GeohashPrefixTree geohashPrefixTree = new GeohashPrefixTree(ctx, MAX_LEVELS);

	public static List<String> getGeoHash(final String geometry_wkt, int precision)
			throws InvalidShapeException, ParseException {

		assert precision <= MAX_LEVELS;

		Shape shape = getShapeFromWKT(geometry_wkt);

		return getGeoHash(shape, precision);
	}

	public static String getGeoHash(final double lat, final double lon, int precision) {

		assert precision <= MAX_LEVELS;

		Point point = getPointFromLatLon(lat, lon);

		return GeohashUtils.encodeLatLon(point.getY(), point.getX(), precision);
	}

	public static Shape getShapeFromWKT(String geometry_wkt) throws InvalidShapeException, ParseException {
		return reader.parse(geometry_wkt);
	}

	public static String getWKTFromLatLon(final double lat, final double lon)
			throws InvalidShapeException, ParseException {

		return GeoUtils.getWKTFromShape(GeoUtils.getPointFromLatLon(lat, lon));
	}

	public static String getWKTFromShape(Shape shape) throws InvalidShapeException, ParseException {
		return writer.toString(shape);
	}

	public static Point getPointFromLatLon(final double lat, final double lon) {
		return ctx.getShapeFactory().pointXY(lon, lat);
	}

	private static List<String> getGeoHash(Shape shape, int precision) {

		List<String> geoHashList = new ArrayList<>();

		CellIterator treeCellIterator = geohashPrefixTree.getTreeCellIterator(shape, precision);

		while (treeCellIterator.hasNext()) {

			String code = treeCellIterator.next().getTokenBytesNoLeaf(null).utf8ToString();

			if (code.length() == precision)
				geoHashList.add(code);
		}

		return geoHashList;
	}

	/**
	 * 
	 * @param value
	 *            Registro avro de donde extraer la geometría
	 * @return Geometría en formato WKT
	 * @throws InvalidShapeException
	 * @throws ParseException
	 */
	public static String getWKTGeometry(SpecificRecord value) throws InvalidShapeException, ParseException {

		// Comprueba si la geometría viene en los diferentes formatos soportados.

		Schema schema = value.getSchema();

		if (schema.getField("longitude") != null && schema.getField("latitude") != null) {

			Object lon = AvroUtils.getSpecificRecordProperty(value, "longitude");
			Object lat = AvroUtils.getSpecificRecordProperty(value, "latitude");

			return getWKTFromLatLon((double) lat, (double) lon);
		}
		if (schema.getField("x") != null && schema.getField("y") != null) {

			Object lon = AvroUtils.getSpecificRecordProperty(value, "x");
			Object lat = AvroUtils.getSpecificRecordProperty(value, "y");

			return getWKTFromLatLon((double) lat, (double) lon);
		}
		if (schema.getField("geometry") != null) {

			return (String) AvroUtils.getSpecificRecordProperty(value, "geometry");
		}
		return null;
	}

	public static boolean shapeContainsGeometry(Shape shape1, Shape shape2) {
		// The shape1 contains the target geometry.
		return (shape1.relate(shape2) == SpatialRelation.CONTAINS);
	}
}
