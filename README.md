# shade

shade (the SHA Drive Engine) stores files in the cloud, in a flexible fashion,
optionally encrypted.

The primary interface is a FUSE filesystem for interacting with shade.  There
is a command line tool "throw" which can cheaply add new files to shade, but
cannot read the encrypted contents.  There is also a command line debugging
tool, shadeutil, for investigating the contents.

## The basic method of file storage
  1. Represent the file as a series of chunks, of a configurable size (16MB by default).
  1. Calculate a SHA-256 hash for each chunk.
  1. Store the chunk in the configured Drive client
  1. Create a manifest file (a shade.File struct) with:
     * Filename
     * Chunk size
     * Indexed list of chunks
  1. Calculate a SHA-256 hash of the manifest.
  1. Store the shade.File in 1 or more Drive implementations (just like Chunk, but retrievable separately).

## Retrieving a file works much the same, just in reverse:
  1. Download all of the manifest files.
  1. Find the filename which matches the request.
  1. If necessary, decrypt the chunk(s).

## shade/drive Drive interface

The Drive interface provides a way to store and retrieve two separate buckets
of bytes, called Files and Chunks, each identified by their sha256sum.  It also
provides a way to list the sha256sum of all known Files.

The interface also provides a bit of metadata about the implementation, such as
a name for identifying it, if it stores files persistently and/or remotely, and
a way to retrieve the configuration that intialized the implementation.

## drive.Drive implementations

There are several implementations of drive.Drive clients.  Some are only for
testing (eg. drive/win, drive/fail), some are for local caching (drive/memory,
drive/local), and some are for remote/cloud storage (drive/amazon,
drive/google).  There are a few special implementations which allow you to
combine (drive/cache) or augment (drive/encrypt) the other implementations.

These implementations can be combined in novel ways by the config package.
Trust your local machine?  You can create a config which will encrypt only the
bytes the leave your machine and go to a remote provider.  Want to always
encrypt bytes at rest?  You can build a config which will encrypt even the
local disk storage, but still cache all File objects unencrypted in memory for
more efficient reads.

## Encryption overview

The drive/encrypt module will encrypt writes to its child client.  It will
AES-256 encrypt the chunked contents of the files, the File objects that
describe the metadata, and even the sha256sums of the chunks.  It then RSA
encrypts the AES-256 key and stores the encrypted key with the File object.

RSA public and private keypairs are provided via the config package.  It is
supported to provide only a public RSA key pair.  This is useful with
cmd/throw/throw.go, which is a "write only" tool which cannot read back any of
the data once it is writen.

For additional details on the implementation, see the godoc for the
drive/encrypt module.

NB: Encrypting the contents stored in Drive comes with two penalties:
  1. Modest CPU usage to encrypt/decrypt on the way in/out.
  1. The chunks of identical files will not be deduplicated.

