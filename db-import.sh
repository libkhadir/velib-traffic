now=$(date +%F)
for i in {0..6}; do 
  currentdate = "$now -$i days"
  curl -O https://velib.nocle.fr/dump/$currentdate-data.db -k; 
done;
