Overview
----

These are my notes on how applyWrite maps writes to chunks.  The examples
intentionally use tiny chunks of 8 bytes, to make it easy to visualize the
boundary conditions.  The unittests do the same.

       0         1         2         3         
       0123456789012345678901234567890123456789
       |--C0---|--C1---|--C2---|--C3---|...
     A <-write->
     B    <---write--->
     C          <---write--->
     D        <---------write--------->

This ascii art was helpful to me to visualize some of the possible ways
incoming writes might map to shade chunks, and the variables that translates
into in the fusefs module's handle.applyWrite() function.

For these examples, the following always holds true:

  - The chunksize is 8.
  - The leading pipe is the first byte of the chunk.
  - The first and last byte of the write are written with < and >, respectively.

Definitions
----

offset = the position of the write relative to the start of the file

writeSize = the length of the write, in bytes

chunkStart = the position of the chunk relative to the start of the file

appendSize = how much of data to append to fill the current chunk

chunkOffset = the position of the chunk, relative to start of the write

dataPtr = the pointer to the next byte to be read from data


Example A
----
    offset = 0
    writeSize = 7

    C0.chunkStart = 0
    C0.chunkOffset = 0
    C0.appendSize = 8
    C0.dataPtr = 0


Example B
----
    offset = 3
    writeSize = 13

    C0.chunkStart = 0
    C0.chunkOffset = 8
    C0.appendSize = 0
    C0.dataPtr = 0

    C1.chunkStart = 8
    C1.chunkOffset = 8
    C1.appendSize = 7
    C1.dataPtr = 5

----

Example C
----
    offset = 9
    writeSize = 13

    C0.chunkStart = 8
    C0.chunkOffset = 1
    C0.appendSize = 0
    C0.dataPtr = 0

    C1.chunkStart = 16
    C1.chunkOffset = 0
    C1.appendSize = 5
    C0.dataPtr = 7

----

Example D
----
    offset = 7
    writeSize = 25

    C0.chunkStart = 0
    C0.chunkOffset = 7
    C0.appendSize = 0
    C0.dataPtr = 0

    C1.chunkStart = 8
    C1.chunkOffset = 0
    C1.appendSize = 0
    C2.dataPtr = 1

    C2.chunkStart = 16
    C2.chunkOffset = 0
    C2.appendSize = 2
    C2.dataPtr = 8

    C3.chunkStart = 24
    C3.chunkOffset = 0
    C3.appendSize = 8
    C3.dataPtr = 16

----
