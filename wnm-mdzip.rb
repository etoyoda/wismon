#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'

SERIES = [
 ['jmagc','/nwp/m0/jmagc[012][0-9].tar.gz'],
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
        next unless /metadata/ === did
        puts rec.inspect
      }
    }
  }
}
