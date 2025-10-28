#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'

SERIES = [
  ['jmagc','/nwp/m0/jmagc[012][0-9].tar.gz'],
  ['devgc','/nwp/m0/devgc[012][0-9].tar.gz'],
  ['devnode','/nwp/m0/devnode[012][0-9].tar.gz'],
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

def wnm_size(wnm)
  if wnm['links']
    clink = nil
    for link in wnm['links']
      clink = link if /^(canonical|update)$/ === link['rel']
    end
    if clink and clink['length']
      return clink['length'].to_i
    end
  end
  if wnm['properties']
    c = wnm['properties']['content']    
    if c and c['size']
      return c['size'].to_i
    end
  end
  nil
end

def gccheck(wnm,selgc)
  return true unless selgc
  return false unless wnm['properties']
  r = wnm['properties']['global-cache'] == selgc
  r
end

$selgc=nil
$selser=nil
$fast=nil
for arg in ARGV
  case arg
  when /^-gc=/ then $selgc=$'
  when /^\w+$/ then $selser=arg
  when /^-fast/ then $fast=true
  else raise "unknown option #{arg}"
  end
end

ts = Hash.new(0)
nsizes = Hash.new(0)
sizes = Hash.new(0)

STDERR.puts "select series #{$selser.inspect}"
STDERR.puts "select global-cache #{$selgc.inspect}"
SERIES.each{|name, path|
  next if $selser and name != $selser
  Dir.glob(path).each{|gzfn|
    STDERR.puts "= #{gzfn}"
    TarReader.open(gzfn){|tar|
      tar.each_entry{|ent|
        json=ent.read
        wnm=JSON.parse(json)
        next unless gccheck(wnm,$selgc)
        topic = fnam_to_topic(ent.name)
        ts[topic] += 1
        size = wnm_size(wnm)
        if size
          nsizes[topic] += 1
          sizes[topic] += size
        end
      }
    }
    break if $fast
  }
}

ts.keys.sort.each do |topic|
  n = ts[topic] || -1
  s = sizes[topic]
  ns = nsizes[topic]
  avgs = s.to_f / ns.to_f * 0.001
  printf("%7u,%11.3f,%s\n", n, avgs, topic)
end
