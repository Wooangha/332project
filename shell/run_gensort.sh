#!/bin/bash

# 사용법:
#   ./run_gensort.sh <n> <max_i> <x> <dirname>
# 예:
#   ./run_gensort.sh 1000000 10 500 inputs

GENSORT="/home/indigo/hj/332project/distributed-sorting/src/test/resources/gensort/64/gensort"
BASEDIR=$(pwd)

if [ $# -ne 4 ]; then
    echo "Usage: $0 <n> <max_i> <x> <dirname>"
    exit 1
fi

n=$1
max_i=$2
x=$3
dirname=$4

# 새 폴더 생성 (이미 있으면 무시)
TARGET_DIR="${BASEDIR}/${dirname}"
mkdir -p "$TARGET_DIR"

for ((i=0; i<max_i; i++)); do
    offset=$((i * n + x))
    outfile="input-${n}-${i}"
    abs_path="${TARGET_DIR}/${outfile}"

    "$GENSORT" -b "${offset}" "${n}" "${abs_path}"

    # 엔터 없이 절대 경로 출력
    printf "%s " "${abs_path}"
done
