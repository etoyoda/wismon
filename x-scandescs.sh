#!/usr/bin/bash
set -xEeuo pipefail
log=/var/www/html/tmp/log.txt
exec > $log 2>&1
cat -n x-scandescs.rb
ruby x-scandescs.rb 301150,307096
ruby x-scandescs.rb 301150,307080
ruby x-scandescs.rb 301150,307091
ruby x-scandescs.rb 301150,309052
ruby x-scandescs.rb 301150,301128,309052,205060
echo done >0
