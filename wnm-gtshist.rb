#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'
require 'time'

SERIES = [
  ['jmagc','/nwp/m0/jmagc[012][0-9].tar.gz'],
  ['devgc','/nwp/m0/devgc[012][0-9].tar.gz'],
  ['devnode','/nwp/m0/devnode[012][0-9].tar.gz'],
]

def fnam_to_topic topic
  topic.sub!(/\.json$/, '')
  topic.sub!(/^(wnm\d{4}-\d{6}|\d{4}[A-Z]{4})-/, '')
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

$selser=nil
$fast=nil
$prev=nil
for arg in ARGV
  case arg
  when /^\w+$/ then $selser=arg
  when /^-fast/ then $fast=true
  when /^-prev=/ then $prev=$'
  else raise "unknown option #{arg}"
  end
end

class GTSHist

  def initialize 
    @db=Hash.new
  end

  def loadprev fnam
    STDERR.puts "loadprev #{fnam}"
    File.open(fnam){|fp|
      for line in fp
        row=line.chomp.split(/,/)
        hdr=row.shift
        @db[hdr]=row
      end
    }
  end

  def register hdr, time
    row=@db[hdr]
    if row.nil? then
      row = [time, time]
    else
      row[1]=time if time>row[1]
    end
    @db[hdr] = row
  end

  def dump fp
    for hdr, row in @db
      fp.puts([hdr, row.join(',')].join(','))
    end
  end

end

gh=GTSHist.new
gh.loadprev($prev) if $prev
STDERR.puts "select series #{$selser.inspect}"
SERIES.each{|name, path|
  next if $selser and name != $selser
  Dir.glob(path).each{|gzfn|
    STDERR.puts "= #{gzfn}"
    TarReader.open(gzfn){|tar|
      tar.each_entry{|ent|
        topic = fnam_to_topic(ent.name)
        next unless /([a-z]{2})-[-a-z]+-gts-to-wis2\/data\/\w+\/(([A-Z]\/){4}\d\d\/[A-Z]{4})/ === topic
        cid, hdr=$1, $2
        hdr = "#{hdr.gsub(/\//, '')}-#{cid}"
        time = Time.at(ent.mtime).utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        gh.register(hdr, time)
      }
    }
    break if $fast
  }
}

gh.dump(STDOUT)
