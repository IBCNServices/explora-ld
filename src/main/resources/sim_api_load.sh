seq 1 200 | xargs -n1 -P10  sh -c 'START=$(date +%s.%N); \
curl "http://10.10.139.5:30070/api/airquality/airquality.no2::number/aggregate/avg/snapshot?ts=1567295940000&src=tiles&res=min&gh_precision=6&bbox=51.311646,4.306641,51.168823,4.504395"; \
END=$(date +%s.%N); \
DIFF=$(echo "$END - $START" | bc); \
echo $DIFF' 