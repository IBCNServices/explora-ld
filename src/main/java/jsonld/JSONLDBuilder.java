package jsonld;

import model.Aggregate;
import util.geoindex.Tile;

import java.util.*;

public class JSONLDBuilder {
    public HashMap<String, Object> buildTile(Tile tile, Long page, Map<String, Aggregate> results,
                                             String aggrMethod, String aggrPeriod) {
        JSONLDDataBuilder dataBuilder = new JSONLDDataBuilder();
        JSONLDDocumentBuilder documentBuilder = new JSONLDDocumentBuilder();
        HashMap blob = documentBuilder.buildTile(tile, page, aggrMethod, aggrPeriod);
        blob.put("@context", JSONLDConfig.CONTEXT);
        blob.put("@graph", dataBuilder.build(results, page, aggrMethod, aggrPeriod));
        return blob;
    }
}
