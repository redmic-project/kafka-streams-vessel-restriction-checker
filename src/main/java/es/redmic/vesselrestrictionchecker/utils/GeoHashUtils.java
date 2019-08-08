package es.redmic.vesselrestrictionchecker.utils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

public class GeoHashUtils {

	private static final int MAX_LEVELS = 4;

	private static SpatialContext ctx = SpatialContext.GEO;

	private static WKTReader reader = new WKTReader(JtsSpatialContext.GEO, new JtsSpatialContextFactory());

	private static GeohashPrefixTree geohashPrefixTree = new GeohashPrefixTree(ctx, MAX_LEVELS);

	public static List<String> getGeoHash(final String geometry_wkt, int precision)
			throws InvalidShapeException, ParseException {

		assert precision <= MAX_LEVELS;

		Shape shape = reader.parse(geometry_wkt);

		return getGeoHash(shape, precision);
	}

	public static String getGeoHash(final double lat, final double lon, int precision) {

		assert precision <= MAX_LEVELS;

		Point point = ctx.getShapeFactory().pointXY(lon, lat);

		return GeohashUtils.encodeLatLon(point.getY(), point.getX(), precision);
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

}
