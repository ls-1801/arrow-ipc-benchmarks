#!/bin/bash


function first_arg () {
 if [ "$1" = "socket" ]; then
   echo "8080"
else
  echo "data"
  fi
}
function executable () {
  type=$1
  file=$2
  shift 2
  if [ "$type" = "py" ]; then
    echo "python3 python/$file.py $@"
  else
    echo "cmake-build-release/$file $@"
  fi
}

lang=(py c++)
mechanisms=(socket fs)
schemas=(wifi icu aq)

for schema in "${schemas[@]}"; do
for source in "${lang[@]}"; do
for sink in "${lang[@]}"; do
for mechansim in "${mechanisms[@]}"; do
  echo "Starting ${source}2${sink}_${mechansim}_${schema}.txt"
  timeout 17s $(executable "${sink}" "${mechansim}_sink" "${schema}" $(first_arg ${mechansim}) 100 4200) &
  timeout 17s $(executable "${source}" "${mechansim}_source" "${schema}" $(first_arg ${mechansim}) 100 4200) &
  wait
  sleep 2
done
done
done
done

