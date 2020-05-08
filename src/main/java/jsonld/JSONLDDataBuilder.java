package jsonld;

import model.Aggregate;

import java.text.SimpleDateFormat;
import java.util.*;

public class JSONLDDataBuilder {
    public List build(Map<String, Aggregate> results, Long page, String aggrMethod, String aggrPeriod) {
        ArrayList<HashMap> graph = new ArrayList<>();
        graph.add(this.buildFeatureOfInterest());
        graph.addAll(this.buildAggregateObservations(results, page, aggrMethod, aggrPeriod));
        return graph;
    }

    private List<HashMap<String, Object>> buildAggregateObservations(Map<String, Aggregate> results, Long page, String aggrMethod, String aggrPeriod) {
        List<HashMap<String, Object>> resultList = new ArrayList<>();
        for (Map.Entry<String, Aggregate> entry : results.entrySet()) {
            String metricId = entry.getKey();
            Aggregate value = entry.getValue();
            HashMap<String, Object> phenomenonTime = new HashMap<>();
            HashMap<String, String> hasBeginning = new HashMap<>();
            HashMap<String, String> hasEnd = new HashMap<>();
            HashMap<String, Object> resultJSON = new HashMap<>();

            hasBeginning.put("inXSDDateTimeStamp", this.getCurrOrNextDate(page, false, aggrPeriod));
            hasEnd.put("inXSDDateTimeStamp", this.getCurrOrNextDate(page, true, aggrPeriod));
            phenomenonTime.put("hasBeginning", hasBeginning);
            phenomenonTime.put("hasEnd", hasEnd);

            resultJSON.put("@id", JSONLDConfig.BASE_URL + metricId + "/" + page);
            resultJSON.put("@type", "sosa:Observation");
            resultJSON.put("hasSimpleResult", value);
            resultJSON.put("resultTime", this.getCurrOrNextDate(page, false, aggrPeriod));
            resultJSON.put("phenomenonTime", phenomenonTime);
            resultJSON.put("observedProperty", JSONLDConfig.BASE_URL + metricId);
            resultJSON.put("usedProcedure", JSONLDConfig.BASE_URL + "id/" + aggrMethod);
            resultJSON.put("hasFeatureOfInterest", JSONLDConfig.BASE_URL + JSONLDConfig.FEATURE_OF_INTEREST);
            resultList.add(resultJSON);
        }
        return resultList;
    }

    private String getCurrOrNextDate(Long page, boolean next, String aggrPeriod){
        Date refPage;
        if (next) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(page);
            switch (aggrPeriod) {
                case "min":
                    cal.add(Calendar.MINUTE, 1);
                    break;
                case "day":
                    cal.add(Calendar.DATE, 1);
                    break;
                case "month":
                    cal.add(Calendar.MONTH, 1);
                    break;
                default:
                    cal.add(Calendar.HOUR, 1);
                    break;
            }
            refPage = cal.getTime();
        } else {
            refPage = new Date(page);
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(refPage);
    }

    private HashMap<String, String> buildFeatureOfInterest() {
        return new HashMap<String, String>(){{
            put("@id", JSONLDConfig.BASE_URL + JSONLDConfig.FEATURE_OF_INTEREST);
            put("@type", "sosa:FeatureOfInterest");
            put("label", JSONLDConfig.FEATURE_OF_INTEREST);
        }};
    }
}
