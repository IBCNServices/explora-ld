package jsonld;

import model.Aggregate;

import java.text.SimpleDateFormat;
import java.util.*;

public class JSONLDDataBuilder {
    public List<LinkedHashMap<String, Object>> build(List<LinkedHashMap<String, Object>> results, Long page, String aggrMethod, String aggrPeriod) throws NoSuchFieldException, IllegalAccessException {
        ArrayList<LinkedHashMap<String, Object>> graph = new ArrayList<>();
        graph.add(this.buildFeatureOfInterest());
//        graph.addAll(this.buildAggregateObservations(results, page, aggrMethod, aggrPeriod));
        graph.addAll(results);
        return graph;
    }

    private List<LinkedHashMap<String, Object>> buildAggregateObservations(Map<String, Aggregate> results, Long page, String aggrMethod, String aggrPeriod) throws NoSuchFieldException, IllegalAccessException {
        List<LinkedHashMap<String, Object>> resultList = new ArrayList<>();
        for (Map.Entry<String, Aggregate> entry : results.entrySet()) {
            String metricId = entry.getKey().split("#")[0];
            String timestamp = entry.getKey().split("#")[1];
            Aggregate value = entry.getValue();
            LinkedHashMap<String, Object> phenomenonTime = new LinkedHashMap<>();
            LinkedHashMap<String, String> hasBeginning = new LinkedHashMap<>();
            LinkedHashMap<String, String> hasEnd = new LinkedHashMap<>();
            LinkedHashMap<String, Object> outputJSON = new LinkedHashMap<>();
            LinkedHashMap<String, Object> resultJSON = new LinkedHashMap<>();

            hasBeginning.put("inXSDDateTimeStamp", this.getCurrOrNextDate(Long.valueOf(timestamp), false, aggrPeriod));
            hasEnd.put("inXSDDateTimeStamp", this.getCurrOrNextDate(Long.valueOf(timestamp), true, aggrPeriod));
            phenomenonTime.put("hasBeginning", hasBeginning);
            phenomenonTime.put("hasEnd", hasEnd);

            outputJSON.put("count", value.getClass().getField("count").get(value));
            outputJSON.put("total", value.getClass().getField("sum").get(value));

            resultJSON.put("@id", JSONLDConfig.BASE_URL + metricId + "/" + timestamp);
            resultJSON.put("@type", "sosa:Observation");
            resultJSON.put("hasSimpleResult", value.getClass().getField(aggrMethod).get(value));
            resultJSON.put("resultTime", this.getCurrOrNextDate(Long.valueOf(timestamp), false, aggrPeriod));
            resultJSON.put("phenomenonTime", phenomenonTime);
            resultJSON.put("observedProperty", JSONLDConfig.BASE_URL + metricId);
            resultJSON.put("madeBySensor", this.convertSensors((HashSet<String>) value.getClass().getField("sensed_by").get(value)));
            resultJSON.put("usedProcedure", JSONLDConfig.BASE_URL + "id/" + aggrMethod);
            resultJSON.put("hasFeatureOfInterest", JSONLDConfig.BASE_URL + JSONLDConfig.FEATURE_OF_INTEREST);
            resultJSON.put("Output", outputJSON);
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

    private LinkedHashMap<String, Object> buildFeatureOfInterest() {
        return new LinkedHashMap<String, Object>(){{
            put("@id", JSONLDConfig.BASE_URL + JSONLDConfig.FEATURE_OF_INTEREST);
            put("@type", "sosa:FeatureOfInterest");
            put("label", JSONLDConfig.FEATURE_OF_INTEREST);
        }};
    }

    private List<String> convertSensors(HashSet<String> sensors) {
        List<String> sensorList = new ArrayList<>();
        for (String sensorId : sensors) {
            sensorList.add(JSONLDConfig.BASE_URL + sensorId);
        }
        return sensorList;
    }
}
