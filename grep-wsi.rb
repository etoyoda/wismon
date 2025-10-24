#!/usr/bin/ruby

for line in ARGF
  next unless /0-(\d\d\d-\d+-[0-9A-Za-z]{1,16}|2000[01]-0-\d{5}|200(0[2-9]|10)-0-[0-9A-Za-z]{1,16})/ === line
  puts $&
end
