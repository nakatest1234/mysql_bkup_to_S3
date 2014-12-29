#!/bin/sh

set -eu

# ======================================================
# memo
# ======================================================
# 1. AWS_CONFIG_FILE=~/.aws/config sh mysql_bkup_to_S3.sh
# 2. --default-character-set=utf8
#    --hex-blob
# ======================================================

PWD=`pwd`
CMD=`basename $0`
YMDHIS=`date +%Y%m%d-%H%M%S`

## check arg
if [ $# -lt 1 ]; then
	echo "Usage: ${CMD} <config>"
	exit 1
fi

### get config abs path
cd `dirname $1`
CONFIG=`pwd`/`basename $1`
cd ${PWD}

### load config
. ${CONFIG}

### check config
CHECK_VAR=(SCHEME SERVER ID PW FLG_GZIP FLG_BKUP_DELETE BKUP_DAYS)
for i in "${CHECK_VAR[@]}"
do
	ii=`eval echo '$'${i}`
	if [ -z "${ii}" ]; then
		echo "CONFIG ERROR: ${i}"
		exit 1
	fi
done

### set prefix
if [ ! -z "${BKUPFILE_PREF}" ]; then
	BKUPFILE_PREF="${BKUPFILE_PREF}."
fi

logger -t ${CMD} START.

### init var
TMPDIR_PARENT=${TMPDIR}/${SERVER}/${SCHEME}
TMPDIR_YMD=${TMPDIR_PARENT}/`date +%Y%m%d`/
OPT_IGNORE=''
OPT_TABLE_LOG=''

### set ignore
if [ ${#TABLE_IGNORE[@]} -gt 0 ]; then
	for i in "${TABLE_IGNORE[@]}"
	do
		OPT_IGNORE="${OPT_IGNORE} --ignore-table=${SCHEME}.${i}"
	done
fi

### set log table
if [ ${#TABLE_LOG[@]} -gt 0 ]; then
	for i in "${TABLE_LOG[@]}"
	do
		OPT_TABLE_LOG="${OPT_TABLE_LOG} ${i}"
		OPT_IGNORE="${OPT_IGNORE} --ignore-table=${SCHEME}.${i}"
	done
fi

if [ ! -d "${TMPDIR_YMD}" ]; then
	mkdir -p ${TMPDIR_YMD}
fi

### get tmpdir abs path
cd ${TMPDIR_YMD}
TMPDIR_YMD=`pwd`
cd ${PWD}

### init bkup files
BKUPFILE_BASE=${TMPDIR_YMD}/${BKUPFILE_PREF}${SERVER}.${SCHEME}.${YMDHIS}
BKUPFILE_CREATE=${BKUPFILE_BASE}.create.sql
BKUPFILE_DATA=${BKUPFILE_BASE}.data.sql
BKUPFILE_LOG=${BKUPFILE_BASE}.log.sql

logger -t ${CMD} START mysqldump.

mysqldump -u ${ID} -p${PW} -h ${SERVER} ${OPT_BASE} ${SCHEME} -d    --result-file=${BKUPFILE_CREATE}
mysqldump -u ${ID} -p${PW} -h ${SERVER} ${OPT_BASE} ${SCHEME} -t -c ${OPT_IGNORE} --result-file=${BKUPFILE_DATA}
if [ ${#TABLE_LOG[@]} -gt 0 ]; then
	mysqldump -u ${ID} -p${PW} -h ${SERVER} ${OPT_BASE} ${SCHEME} ${OPT_TABLE_LOG} -t -c --result-file=${BKUPFILE_LOG}
fi

if [ "${FLG_RESET_AUTOINCREMENT}" = "y" ]; then
	sed -i 's/\s\+AUTO_INCREMENT=[0-9]\+//' ${BKUPFILE_CREATE}
fi

logger -t ${CMD} DONE mysqldump.

if [ "${FLG_GZIP}" = "y" ]; then
	logger -t ${CMD} START gzip.

	nice gzip ${BKUPFILE_BASE}*.sql

	logger -t ${CMD} END gzip.

	if [ ! -z "${S3_DIR}" ]; then
		logger -t ${CMD} START cp to S3.

		aws s3 cp --quiet ${BKUPFILE_CREATE}.gz s3://${S3_DIR}/
		aws s3 cp --quiet ${BKUPFILE_DATA}.gz s3://${S3_DIR}/
		if [ ${#TABLE_LOG[@]} -gt 0 ]; then
			aws s3 --quiet cp ${BKUPFILE_LOG}.gz s3://${S3_DIR}/
		fi

		logger -t ${CMD} END cp to S3.
	fi
else
	if [ ! -z "${S3_DIR}" ]; then
		logger -t ${CMD} START cp to S3.

		aws s3 cp --quiet ${BKUPFILE_CREATE} s3://${S3_DIR}/
		aws s3 cp --quiet ${BKUPFILE_DATA} s3://${S3_DIR}/
		if [ ${#TABLE_LOG[@]} -gt 0 ]; then
			aws s3 cp --quiet ${BKUPFILE_LOG} s3://${S3_DIR}/
		fi

		logger -t ${CMD} END cp to S3.
	fi
fi

if [ "${FLG_BKUP_DELETE}" = "y" ]; then
	find ${TMPDIR_PARENT} -type f -mtime +${BKUP_DAYS} -exec rm -f "{}" \;
	find ${TMPDIR_PARENT} -type d -empty -print0 | xargs --no-run-if-empty -0 rmdir
fi

logger -t ${CMD} END.

exit 0
