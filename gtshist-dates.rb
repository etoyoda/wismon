#!/usr/bin/ruby
require 'time'
fstat=Hash.new()
lstat=Hash.new()
for line in ARGF
  dset,fdate,ldate=line.chomp.split(/,/, 3)
  #next unless /-jp$/ =~ dset
  tt=dset[0,2]
  fdate=Time.parse(fdate).utc.strftime('%Y-%m-%d')
  fstat[fdate]=Hash.new(0) unless fstat.include?(fdate)
  fstat[fdate][tt]+=1
  ldate=Time.parse(ldate).utc.strftime('%Y-%m-%d')
  lstat[ldate]=Hash.new(0) unless lstat.include?(ldate)
  lstat[ldate][tt]+=1
end

def report date, h
  tot=0
  for n in h.values
    tot+=n
  end
  a = h.keys.sort{|a,b|h[b]<=>h[a]}.map{|tt|
    sprintf('%s:%02u%%', tt, (h[tt]*100.0/tot+0.5).to_i)
  }
  printf("%s,%6u,%s\n", date, tot, a[0,10].join(','))
end

puts "= date of first appearance"
for date in fstat.keys.sort
  report date, fstat[date]
end
puts "= date of last appearance"
for date in lstat.keys.sort
  report date, lstat[date]
end
