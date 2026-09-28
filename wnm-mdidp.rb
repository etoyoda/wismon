#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'
require 'gdbm'

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
        mdid = rec['properties']['metadata_id']
        puts mdid
      }
    }
  }
}
