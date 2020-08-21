#!/bin/sh

ROOT="$1"
if [ -z "$ROOT" ]; then
	echo "usage: $0 PROFILE"
	exit 1
fi

TOTAL_LOGS=$(find "$ROOT" -name "crawl.log" | wc -l)

ERROR_LOG_FILES=$(find "$ROOT" -name "crawl.log" | xargs grep -o "^ERROR .*" | cut -d: -f1 | sort -u)
FATAL_LOG_FILES=$(find "$ROOT" -name "crawl.log" | xargs grep -o "FATAL:[^:][^:]*:.*" | cut -d: -f1 | sort -u)
# warning: this is not quite complete (wait for PG_LOG_ASSERT build to be used), but it's _close_
PG_FATAL_LOG_FILES=$(find "$ROOT" -name "crawl.log" | xargs grep -o "FATAL:page_graph.*" | cut -d: -f1 | sort -u)

(find "$ROOT" -name "crawl.log" | xargs grep -o "^ERROR .*" | cut -d: -f1 | sort -u >tmp_error_logs.txt) || exit 1
(find "$ROOT" -name "crawl.log" | xargs grep -o "FATAL:[^:][^:]*:.*" | cut -d: -f1 | sort -u >tmp_fatal_logs.txt) || exit 1
(find "$ROOT" -name "crawl.log" | xargs grep -o "FATAL:page_graph.*" | cut -d: -f1 | sort -u >tmp_pg_fatal_logs.txt) || exit 1

ALL_ISSUE_COUNT=$(comm --total -123 tmp_error_logs.txt tmp_fatal_logs.txt | awk '{print $1 + $2 + $3}')
ERROR_ONLY_COUNT=$(comm -23 tmp_error_logs.txt tmp_fatal_logs.txt | wc -l)
NON_PG_FATAL_COUNT=$(comm -23 tmp_fatal_logs.txt tmp_pg_fatal_logs.txt | wc -l)
PG_FATAL_COUNT=$(wc -l <tmp_pg_fatal_logs.txt)

ALL_ISSUE_PCT=$(dc -e "4 k $ALL_ISSUE_COUNT $TOTAL_LOGS / 100 * p")
ERROR_ONLY_PCT=$(dc -e "4 k $ERROR_ONLY_COUNT $TOTAL_LOGS / 100 * p")
NON_PG_FATAL_PCT=$(dc -e "4 k $NON_PG_FATAL_COUNT $TOTAL_LOGS / 100 * p")
PG_FATAL_PCT=$(dc -e "4 k $PG_FATAL_COUNT $TOTAL_LOGS / 100 * p")

echo ">> Profile: $ROOT"
echo ">> $TOTAL_LOGS total logs"
echo ">> $ALL_ISSUE_COUNT ($ALL_ISSUE_PCT%) had some kind of ERROR/FATAL"
echo ">> $ERROR_ONLY_COUNT ($ERROR_ONLY_PCT%) had ERRORs without FATALs"
echo ">> $NON_PG_FATAL_COUNT ($NON_PG_FATAL_PCT%) had non-PG-related FATALs"
echo ">> $PG_FATAL_COUNT ($PG_FATAL_PCT%) had PG-related FATALs"
echo "-----------------------------------------"

rm tmp_pg_fatal_logs.txt tmp_fatal_logs.txt tmp_error_logs.txt
