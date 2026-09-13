#!/usr/bin/ruby
require 'tarreader'
require 'tarwriter'
require 'digest/md5'

for fnam in ARGV
  h=Hash.new
  nf=0
  nd=0
  ofnam=File.basename(fnam).sub(/\.tgz$/,'.tar').sub(/\.gz$/,'')
  ofnam.sub(/$/,'.tmp') if fnam==ofnam
  TarReader.open(fnam){|tar|
    warn "writing #{ofnam} from #{fnam}"
    TarWriter.open(ofnam, 'w'){|otar|
      tar.each_entry{|ent|
        data=ent.read
        sum=Digest::MD5.hexdigest(data)
        if h[sum] then
          nd+=1
        else
          h[sum]=ent.name
          otar.add(ent.name,data)
        end
        nf+=1
      }
    }
  }
  warn("gzip -f -9 #{ofnam}")
  system("gzip -f -9 #{ofnam}")
  printf("nf=%u nd=%u (dup %5.1f%%) %s\n", nf, nd, nd*100.0/nf, fnam)
end
