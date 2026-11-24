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

pax_path = nil
Dir.glob(path).each{|gzfn|
  TarReader.open(gzfn){|tar|
    tar.each_entry{|ent|
      if ent.name.include?('@PaxHeader') or ent.name.include?('PaxHeaders.')
        pax = ent.read.to_s
        pax_path = pax[/^\d+ path=(.*)$/, 1]
        next
      end
      name = pax_path || ent.name
      pax_path = nil
      next unless rtopic === name
      json=ent.read
      next if json.nil?
      rec=JSON.parse(json)
      did = rec['properties']['data_id']
      puts did
    }
  }
}
