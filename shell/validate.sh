#!/bin/bash

# 사용법:
#   ./validate_global.sh data1 data2 data3 ...
#
# 디렉토리 순서대로 key가 작은 순임.
# 예: ./validate_global.sh data1 data2 data3

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
VALSORT="${SCRIPT_DIR}/../distributed-sorting/src/test/resources/gensort/64/valsort"

# 전체 all.sum 초기화
rm -f all.sum

for DIR in "$@"; do
    echo "======================================"
    echo "Processing directory: $DIR"
    echo "======================================"

    if [ ! -d "$DIR" ]; then
        echo "Skipping $DIR (not a directory)"
        continue
    fi

    # 기존 sum 삭제
    rm -f "$DIR"/out*.sum

    # partition.* 순회 (번호 순서대로)
    for file in $(ls "$DIR"/partition.* | sort -V); do
        basename=$(basename "$file")     # partition.X
        idx=${basename#partition.}       # X
        sumfile="$DIR/out${idx}.sum"

        echo "  valsort -o $sumfile $file"
        "${VALSORT}" -o "$sumfile" "$file"

        # 디렉토리마다 순서대로 all.sum에 append
        echo "  appending $sumfile → all.sum"
        cat "$sumfile" >> all.sum
    done

    echo ""
done

echo "======================================"
echo "Final check: valsort -s all.sum"
echo "======================================"
"${VALSORT}" -s all.sum
