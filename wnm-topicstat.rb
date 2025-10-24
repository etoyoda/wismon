#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'

SERIES = [
 ['DevGC','/nwp/m0/jmagc[012][0-9].tar.gz'],
# ['DevNode','/nwp/m0/devnode[012][0-9].tar.gz'],
]

=begin
require 'gdbm'
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
    when /^urn:wmo:md:([-\w]+):(synop|automaticas|2obre1|uspxz9|3v3tr2)$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
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
    when /^W_XX-EUMETSAT-Darmstadt,SOUNDING\+SATELLITE,([0-9A-Z]+)\+([0-9A-Za-z]+)_/
      topic = "fake/a/wis2/int-eumetsat/data/core/space-based-observations/#{$1.downcase}/#{$2.downcase}-sounding"
    when /^W_(XX-EUMETSAT-Darmstadt|FR-CNES-Toulouse),SURFACE\+SATELLITE,([0-9A-Z]+)\+([+0-9A-Z]+)_/
      centreid,n2,n3 = $1,$2,$3
      centreid = case centreid when /^XX/ then 'int-eumetsat' when /^FR/ then 'fr-cnes' else centreid end
      topic = "fake/a/wis2/#{centreid}/data/core/space-based-observations/#{n2.downcase}/#{n3.downcase.gsub(/\+/,'-')}-surface"
    when /^W_XX-EUMETSAT-Darmstadt,SING\+LEV\+SAT,([0-9A-Z]+)\+([A-Z]+)_/
      topic = "fake/a/wis2/int-eumetsat/data/core/space-based-observations/#{$1.downcase}/#{$2.downcase}-singlev"
    when /^W_XX-EUMETSAT-Darmstadt,IMG\+SAT,([0-9A-Z]+)\+([-0-9A-Z]+)_/
      topic = "fake/a/wis2/int-eumetsat/data/core/space-based-observations/#{$1.downcase}/#{$2.downcase.gsub(/-+/,'-').sub(/-BUFR/,'')}-surface"
    when /^W_XX-EUMETSAT-([a-z]+),([a-z]+),DBNet\+([0-9a-z]+)/
      topic = "fake/a/wis2/int-eumetsat-#{$1}/data/core/space-based-observations/#{$3.downcase}/#{$2.downcase}"
    end
  end
  topic
end
=end

def retrtopic topic
  topic.sub!(/\.json$/, '')
  topic.sub!(/^(wnm\d{4}-\d{6}|\d{4}[A-Z]{4})-/, '')
  topic = '(gts)' if /-gts-to-wis2_/ === topic
  topic.sub!(/_d_c_w_p_a_/, '_data_core_weather_prediction_analysis_')
  topic.sub!(/_d_c_w_p_f_/, '_data_core_weather_prediction_forecast_')
  topic.sub!(/_d_c_w_p_forecast/, '_data_core_weather_prediction_forecast')
  topic.sub!(/_d_c_w_s_sentinel/, '_data_core_weather_space-based-observations_sentinel')
  topic.sub!(/_d_c_w_/, '_data_core_weather_')
  topic.sub!(/_d_c_/, '_data_core_')
  topic.sub!(/_d_/, '_data_')
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
        topic = retrtopic(ent.name)
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
