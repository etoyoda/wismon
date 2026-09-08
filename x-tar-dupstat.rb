#!/usr/bin/ruby
require 'tarreader'
require 'digest/md5'


for fnam in ARGV
  h=Hash.new
  nf=0
  nd=0
  TarReader.open(fnam){|tar|
    tar.each_entry{|ent|
      sum=Digest::MD5.hexdigest(ent.read)
      nd+=1 if h[sum]
      h[sum]=ent.name
      nf+=1
    }
  }
  printf("nf=%u nd=%u (dup %5.1f%%) %s\n", nf, nd, nd*100.0/nf, fnam)
end
