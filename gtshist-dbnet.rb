#!/usr/bin/ruby
require 'time'

db=Hash.new

for line in ARGF
  dset,fdate,ldate=line.chomp.split(/,/, 3)
  next unless /^IN/ =~ dset
  tta=dset[0,3]
  c4=dset[6,4]
  gw=dset[11,2]
  raise gw unless /^(jp|de)$/ === gw
  key=[c4,tta].join(' ')
  db[key]=['-','-',0,0] unless db[key]
  db[key][0]=ldate if ldate>db[key][0] and gw=='de'
  db[key][1]=ldate if ldate>db[key][1] and gw=='jp'
  db[key][2]+=1 if gw=='de'
  db[key][3]+=1 if gw=='jp'
end

for key in db.keys.sort
  printf "%s\t%u\t%21s\t%u\t%21s\n", key, db[key][2], db[key][0], db[key][3], db[key][1]
end
