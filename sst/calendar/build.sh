#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
source "$here/../../../SST/env.sh"
mkdir -p "$1"
lib=$(mktemp "$1/.wsecalendar-XXXXXX.so")
trap 'rm -f -- "$lib"' EXIT
"$SST_CXX" $(sst-config --ELEMENT_CXXFLAGS) -Wall -Wextra -O2 \
    "$here/calendar.cc" $(sst-config --ELEMENT_LDFLAGS) -o "$lib"
mv -f -- "$lib" "$1/libwsecalendar.so"
