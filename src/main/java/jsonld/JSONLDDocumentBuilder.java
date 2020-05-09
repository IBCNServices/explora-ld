package jsonld;

import org.apache.kafka.common.protocol.types.Field;
import util.geoindex.Tile;

import java.text.SimpleDateFormat;
import java.util.*;

public class JSONLDDocumentBuilder {
    public HashMap<String, Object> buildTile(Tile tile, Long page, String aggrMethod, String aggrPeriod) {
        HashMap<String, Object> tileInfoObj = this.buildTilesInfo(tile, page, aggrMethod, aggrPeriod);
        HashMap<String, Object> dcTermsInfoObj = this.buildDctermsInfo(tile, aggrMethod, aggrPeriod);
        dcTermsInfoObj.forEach((k, v) -> tileInfoObj.merge(k, v, (v1, v2) -> v2));
        return tileInfoObj;
    }

    private HashMap<String, Object> buildTilesInfo(Tile tile, Long page, String aggrMethod, String aggrPeriod) {
        Date prevPage = this.getPrevOrNextDate(page, true, aggrPeriod);
        Date nextPage = this.getPrevOrNextDate(page, false, aggrPeriod);
        HashMap result = new HashMap();
        result.put("@id", this.buildTileURI(tile, this.getFormattedDate(new Date(page)), aggrMethod, aggrPeriod));
        result.put("tiles:zoom", tile.getZoom());
        result.put("tiles:longitudeTile", tile.getX());
        result.put("tiles:latitudeTile", tile.getY());
        result.put("startDate", this.getFormattedDate(new Date(page)));
        result.put("endDate", this.getFormattedDate(nextPage));
        result.put("previous", this.buildTileURI(tile, this.getFormattedDate(prevPage), aggrMethod, aggrPeriod));

        if (nextPage.before(new Date())) {
            result.put("next", this.buildTileURI(tile, this.getFormattedDate(nextPage), aggrMethod, aggrPeriod));
        }
        return result;
    }

    private Date getPrevOrNextDate(Long page, boolean prev, String aggrPeriod){
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(page);
        switch (aggrPeriod) {
            case "min":
                cal.add(Calendar.MINUTE, prev? -1:1);
                break;
            case "day":
                cal.add(Calendar.DATE, prev? -1:1);
                break;
            case "month":
                cal.add(Calendar.MONTH, prev? -1:1);
                break;
            default:
                cal.add(Calendar.HOUR, prev? -1:1);
                break;
        }
        Date refPage = cal.getTime();
        return refPage;
    }

    private String getFormattedDate(Date page) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(page);
    }

    private String buildTileURI(Tile tile, String page, String aggrMethod, String aggrPeriod) {
        return "http://" + System.getenv("REST_ENDPOINT_HOSTNAME") + ":" + System.getenv("REST_ENDPOINT_PORT") + "/data/" + tile.getZoom() + "/" + tile.getX() + "/" + tile.getY() + "?page=" + page + "&aggrMethod=" + aggrMethod + "&aggrPeriod=" + aggrPeriod;
    }

    private HashMap<String, Object> buildDctermsInfo(Tile tile, String aggrMethod, String aggrPeriod) {
        String id = "http://" + System.getenv("REST_ENDPOINT_HOSTNAME") + ":" + System.getenv("REST_ENDPOINT_PORT") + "/data/" + tile.getZoom() + "/" + tile.getX() + "/" + tile.getY() + "?aggrMethod=" + aggrMethod + "&aggrPeriod=" + aggrPeriod;
        HashMap<String, Object> dcTermsInfoObj = new HashMap<>();
        HashMap<String, Object> dcIsPartOf = new HashMap<>();
        HashMap<String, Object> hydraSearch = new HashMap<>();

        hydraSearch.put("@type", "hydraIriTemplate");
        hydraSearch.put("hydra:template", "http://" + System.getenv("REST_ENDPOINT_HOSTNAME") + ":" + System.getenv("REST_ENDPOINT_PORT") + "/data/" + tile.getZoom() + "/" + tile.getX() + "/" + tile.getY() + "?aggrMethod=" + aggrMethod + "&aggrPeriod=" + aggrPeriod);
        hydraSearch.put("hydra:variableRepresentation", "hydra:BasicRepresentation");
        hydraSearch.put("hydra:mapping", this.buildHydraMapping());
        dcIsPartOf.put("@id", id);
        dcIsPartOf.put("@type", "hydra:Collection");
        dcIsPartOf.put("dcterms:license", "");
        dcIsPartOf.put("dcterms:right", "");
        dcIsPartOf.put("hydra:search", hydraSearch);
        dcTermsInfoObj.put("dcterms:isPartOf", dcIsPartOf);

        return dcTermsInfoObj;
    }

    private List<HashMap<String, Object>> buildHydraMapping() {
        List<HashMap<String, Object>> result = new ArrayList<>();

        HashMap<String, Object> xMapping = new HashMap<>();
        HashMap<String, Object> yMapping = new HashMap<>();
        HashMap<String, Object> pageMapping = new HashMap<>();
        HashMap<String, Object> aggrMethodMapping = new HashMap<>();
        HashMap<String, Object> aggrPeriodMapping = new HashMap<>();

        xMapping.put("@type", "hydra:IriTemplateMapping");
        xMapping.put("hydra:variable", "x");
        xMapping.put("hydra:property", "tiles:longitudeTile");
        xMapping.put("hydra:required", true);

        yMapping.put("@type", "hydra:IriTemplateMapping");
        yMapping.put("hydra:variable", "y");
        yMapping.put("hydra:property", "tiles:latitudeTile");
        yMapping.put("hydra:required", true);

        pageMapping.put("@type", "hydra:IriTemplateMapping");
        pageMapping.put("hydra:variable", "page");
        pageMapping.put("hydra:property", "dcterms:date");
        pageMapping.put("hydra:required", false);

        aggrMethodMapping.put("@type", "hydra:IriTemplateMapping");
        aggrMethodMapping.put("hydra:variable", "aggrMethod");
        aggrMethodMapping.put("hydra:property", "dcterms:accrualMethod");
        aggrMethodMapping.put("hydra:required", true);

        aggrPeriodMapping.put("@type", "hydra:IriTemplateMapping");
        aggrPeriodMapping.put("hydra:variable", "aggrPeriod");
        aggrPeriodMapping.put("hydra:property", "dcterms:accrualPeriodicity");
        aggrPeriodMapping.put("hydra:required", true);

        result.add(xMapping);
        result.add(yMapping);
        result.add(pageMapping);
        result.add(aggrMethodMapping);
        result.add(aggrPeriodMapping);

        return result;
    }

}
