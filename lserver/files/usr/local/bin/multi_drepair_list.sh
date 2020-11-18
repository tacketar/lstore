#!/bin/bash

if [ "${2}" == "" ]; then
    echo "${0} dX1/rid_1 .. dXN/rid_N"
    exit 1
fi

fname=""
for i in $*; do
    depot=$(echo ${i} | cut -f1 -d/)
    rid=$(echo ${i} | cut -f2 -d/)
    echo "Processing RID ${depot}/${rid}"
    warmer_query.py --fname --prefix "@:" --rid ${rid} > /tmp/${depot}r${rid}.$$
    echo "      $(wc -l /tmp/${depot}r${rid}.$$ | cut -f1 -d\  ) files"
    if [ "${fname}" == "" ]; then
        fname="${depot}r${rid}"
    else
        fname="${fname}_${depot}r${rid}"
    fi
done

fname="${fname}"
echo "Composite name: ${fname}"

cat /tmp/*.$$ | sort | uniq > /tmp/${fname}.w
rm /tmp/*.$$

echo "Total files: $(wc -l /tmp/${fname}.w | cut -f1 -d\  )"

