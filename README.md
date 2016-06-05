# shade
shade (the SHA Drive Engine) stores files in the cloud, in a flexible fashion, optionally encrypted.

## The basic method of file storage is:
  1. Represent the file as a series of chunks, of a configurable size (16MB by default).
  1. Calculate a SHA-256 hash for each chunk.
  1. Optionally, generate a unique AES-128 key for the file and encrypt each chunk with it.
  1. Store the chunk in 1 or more cloud "Drive" implementations (initial support for Google Drive and Amazon Cloud Drive).
  1. Create a manifest file, storing:
    * Filename
    * Chunk size
    * Indexed list of chunks
    * Optionally, the chosen AES encryption key
  1. Calculate a SHA-256 hash of the manifest.
  1. Optionally, encrypt the manifest with an RSA public key.
  1. Store the manifest in 1 or more cloud "Drive" implementations (as above, but a seperate folder).

NB: Encrypting the contents stored in Drive comes with two penalties:
  1. Modest CPU usage to encrypt/decrypt on the way in/out.
  1. The chunks of identical files will not be deduplicated.

## Retrieving a file works much the same, just in reverse:
  1. Download all of the manifest files.
  1. If necessary, decrypt the manifest files.
  1. Find the filename which matches the request.
  1. Retrieve the chunk(s) described in the manifest.
  1. If necessary, decrypt the chunk(s).
