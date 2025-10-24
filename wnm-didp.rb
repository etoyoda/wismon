#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'

SERIES = [
 ['DevGC','/nwp/m0/devgc[012][0-9].tar.gz'],
# ['DevNode','/nwp/m0/devnode[012][0-9].tar.gz'],
]



SERIES.each{|name, path|
  puts "= #{name}"
  Dir.glob(path).each{|gzfn|
    TarReader.open(gzfn){|tar|
      tar.each_entry{|ent|
        json=ent.read
        next if json.nil?
        rec=JSON.parse(json)
        did = rec['properties']['data_id']
        puts did
      }
    }
  }
}
