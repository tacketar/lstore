#Helpers for generating files from the warmer. Should be sourced from the other scripts

#Determinw which DB to use
if [ "$(pgrep lio_warm)" != "" ]; then
    DB=/lio/log/warm.2
else
    DB=/lio/log/warm
fi

echo "Using warmer DB: ${DB}"

warmer_query_fname_by_rid() {
    warmer_query -db ${DB} -fonly -r ${1} | sed 's/^\//@:\//g'
}

warmer_query_write_errors() {
    warmer_query -db ${DB} -fonly -w | sed 's/^\//@:\//g'
}


