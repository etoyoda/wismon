#!/usr/bin/ruby

h=Hash.new(0)

for line in ARGF
  n,tc=line.strip.split(/ /,2)
  cid=tc.split(/\//)[3]
  next unless cid
  h[cid]+=1
end

for cid in h.keys.sort
  printf("%s\t%s\n", cid, h[cid])
end

