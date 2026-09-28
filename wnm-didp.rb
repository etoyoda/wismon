#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'

path='/nwp/m0/jmagc[012][0-9].tar.gz'

topic='.'

for arg in ARGV
  case arg
  when /^--path=/ then path = $'
  when /^--topic=/ then topic = $'
  end
end

rtopic=Regexp.new(topic)

Dir.glob(path).each{|gzfn|
  TarReader.open(gzfn){|tar|
    tar.each_entry{|ent|
      next unless rtopic === ent.name 
      json=ent.read
      next if json.nil?
      rec=JSON.parse(json)
      did = rec['properties']['data_id']
      puts did
    }
  }
}
