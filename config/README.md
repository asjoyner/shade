
# Shade Configuration

All Shade tools read a single JSON configuration file which must contain a
representation of one shade Drive object.  There are several example configs in
the testdata/ subdirectory.  These demonstrate bad configs, very basic configs
for single Drive implementations, and a practical full config for storing
encrypted contents in Google Drive.

Pull requests demonstrating novel configurations are welcome.  :)

*Tip:* `shadeutil genkeys -t N` will generate RSA keys and print them as
properly formatted JSON strings, for use with the "encrypt" client.
