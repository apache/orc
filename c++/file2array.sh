#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

# Convert file to a C++11 vector of unsigned char
#
set -e

arrname=""
modifiers=""

usage () {
  echo "Usage: $0 [options] [input file] " >&2
  echo "Options:" >&2
  echo "  -v <c array variable name>" >&2
  echo "     Name of C variable in output file. Must be provided." >&2
  echo "  -m <array variable modifiers>" >&2
  echo "     Modifiers for C variable in output file. Default is const." >&2
  exit 1
}

while getopts "m:v:" opt; do
  case $opt in
    m)
      if [[ $modifiers != "" ]]; then
        echo "-m specified twice" >&2
        usage
      fi
      modifiers=$OPTARG
      ;;
    v)
      if [[ $arrname != "" ]]; then
        echo "-v specified twice" >&2
        usage
      fi
      arrname=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      usage
  esac
done
shift $((OPTIND - 1))

infile=$1
if [[ $# > 1 ]]; then
  echo "Too many remaining arguments: $@" >&2
  usage
fi

if [ -z "$arrname" ]; then
  echo "-v not provided or empty." >&2
  usage
fi

if [ -z "$modifiers" ]; then
  # Default is const with global linking visibility
  modifiers="const"
fi

echo "#include <vector>" # For size_t
echo
# Preceding extern declaration guarantees external linkage in C++
echo "extern $modifiers std::vector<unsigned char> $arrname;";
echo
echo "$modifiers std::vector<unsigned char> $arrname = {"
xxd -i < $infile
echo "};"
