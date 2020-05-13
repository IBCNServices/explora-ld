package jsonld;

import model.Aggregate;
import util.geoindex.Tile;

import java.util.*;

public class JSONLDBuilder {
    public  LinkedHashMap<String, Object> buildTile(Tile tile, Long page, Map<String, HashMap> results,
                                             String aggrMethod, String aggrPeriod) {
        JSONLDDataBuilder dataBuilder = new JSONLDDataBuilder();
        JSONLDDocumentBuilder documentBuilder = new JSONLDDocumentBuilder();
        LinkedHashMap<String, Object> blob = documentBuilder.buildTile(tile, page, aggrMethod, aggrPeriod);
        blob.put("@context", JSONLDConfig.CONTEXT);
        blob.put("@graph", dataBuilder.build(results, page, aggrMethod, aggrPeriod));
        return blob;
    }
}
