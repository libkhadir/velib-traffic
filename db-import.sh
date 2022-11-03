now=$(date +%F)
for i in {0..6}; do 
  currentdate=$(date -d "$now -$i days" +"%F")
  echo "importing data for $currentdate"
  curl -O https://velib.nocle.fr/dump/$currentdate-data.db -k; 
done;
