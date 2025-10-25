#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'

SERIES = [
 ['DevGC','/nwp/m0/jmagc[012][0-9].tar.gz'],
# ['DevNode','/nwp/m0/devnode[012][0-9].tar.gz'],
]

def fnam_to_topic topic
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
  topic.gsub!(/_/, '/')
  topic
end

ts = Hash.new(0)
sizes = Hash.new(0)

SERIES.each{|name, path|
  Dir.glob(path).each{|gzfn|
    STDERR.puts "= #{gzfn}"
    TarReader.open(gzfn){|tar|
      tar.each_entry{|ent|
        json=ent.read
        rec=JSON.parse(json)
        topic = fnam_to_topic(ent.name)
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
