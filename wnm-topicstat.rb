#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'
require 'gdbm'

SERIES = [
 ['DevGC','/nwp/m0/jmagc[012][0-9].tar.gz'],
# ['JmaGC','/nwp/m0/jmagc[012][0-9].tar.gz'],
# ['DevNode','/nwp/m0/devnode[012][0-9].tar.gz'],
]

gdbm = GDBM.new('/nwp/m0/mdtopic.gdbm', 0, GDBM::READER)

def guess_topic rec, gdbm
  mdid = rec['properties']['metadata_id']
  topic = if mdid then gdbm[mdid] else nil end
  if topic.nil? then
    case mdid
    when /^urn:wmo:md:([-\w]+):core\.surface-based-observations\.synop$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):upperair-weather-observations:([-\w]+)$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/#$2"
    when /^urn:wmo:md:([-\w]+):surface-weather-observations(:synop)?$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):surface-based-observations\.synop$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):surface-based-observations:temp$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/temp"
    when /^urn:wmo:md:([-\w]+):observations\.surface\.synop-bufr$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):data\.core\.weather\.surface-based-observations\.synop$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):synop-dataset-surface-observations$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:x-wmo:md:int-ecmwf:open-data:\d+:\d\dz:0p25:\w\w:\d+h$/
      topic = "dummy/a/wis2/int-ecmwf/data/core/weather/prediction/forecast"
    end
  end
  if topic.nil? then
    case rec['properties']['data_id']
    when /^([-\w]+)\/metadata\/urn:wmo:md/
      topic = '(metadata)'
    when /^wis2\/\w+-\w+-gts-to-wis2\//
      topic = '(gts-gw)'
    when /^weather\/surface-based-observations\/(\w+)\/A_[A-Z]{4}\d\d(\w{4})/
      topic = "(gts #$2)"
    when /^weather\/surface-based-observations\/(\w+)\/A_[A-Z]{4}\d\d(\w{4})/
    when /^origin\/a\/wis2\/([-\w]+)\/data\/(\w+)\/([-\w]+)\/([-\w]+)\/([-\w]+)/
      topic = "fake/a/wis2/#$1/data/#$2/#$3/#$4/#$5"
    when /^([-\w]+)\/data\/(core|recommended)\/(weather)\/([-\w]+)\/([-\w]+)/
      topic = "fake/a/wis2/#$1/data/#$2/#$3/#$4/#$5"
    end
  end
  topic
end

ts = Hash.new(0)

SERIES.each{|name, path|
  Dir.glob(path).each{|gzfn|
    STDERR.puts "= #{gzfn}"
    TarReader.open(gzfn){|tar|
      tar.each_entry{|ent|
        json=ent.read
        rec=JSON.parse(json)
        mdid = rec['properties']['metadata_id']
        topic = guess_topic(rec, gdbm)
        if !topic
          if mdid then
            STDERR.puts "unresolvable mdid=#{mdid}"
          else
            STDERR.puts "unresolvable dataid=#{rec['properties']['data_id']}"
          end
        end
        ts[topic] = 0 unless ts.include?(topic)
        ts[topic] += 1
      }
    }
  }
}

if ts.include?(nil) then
  nnil = ts[nil]
  ts.delete(nil)
  ts['(nil)']=nnil
end
ts.keys.sort.each do |topic|
  n = ts[topic]
  printf("%7u %s\n", n || -1, topic)
end
