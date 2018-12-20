#!/bin/bash

# This very strongly assumes you will execute it as ./regression_test.sh.

BS=20971520  # 20MBytes (20 * 1024 * 1024)
dd if=/dev/urandom of=testdata/testinput bs=${BS} count=3 2>/dev/null && 
  rm -f testdata/got/{files,chunks}/* &&
  go build &&
  ./throw --config=testdata/config.json testdata/testinput testfilename

# Please allow me to apologize for this next chunk of code in advance.
# This ghetto-decodes JSON to extract an ordered list of chunk filenames to
# reassemble the file and compare with the original.  The filenames are
# translated from the base64 encoding used in JSON to the hex encoding of the
# local drive client.
# Please send a better test as a pull request.  :)
rm -f testdata/testoutput
cat testdata/got/files/* | sed -e 's/,/\n/g' | grep Sha256 | cut -f4 -d \" |
  while read line; do
    FN=$(echo $line | base64 -d | xxd -ps | xargs | sed -e 's/ //g')
    cat testdata/got/chunks/${FN} >> testdata/testoutput
  done

diff testdata/testinput testdata/testoutput
RESULT=$?
if [ ${RESULT} == 0 ]; then
  echo "Pass!"
  rm -f testdata/testinput testdata/testoutput testdata/got/{files,chunks}/*
else
  echo "Fail!"
fi

