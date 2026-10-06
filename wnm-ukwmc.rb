#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'

class App

  def initialize argv
    @path='/nwp/m0/{us,fr}gb[012][0-9].tar.gz'
    topic='uk-metoffice-wmc'
    for arg in argv
      case arg
      when /^--path=/ then @path = $'
      when /^--topic=/ then topic = $'
      end
    end
    @rtopic=Regexp.new(topic)
  end

  def did_parse did
    sl_sections=did.split(/\//)
    case sl_sections.size
    when 7 then
      pn,el,af,lv,ft,fnam,stime=sl_sections
    when 6 then
      lv=nil
      pn,el,af,ft,fnam,stime=sl_sections
    when 0..5 then
      raise "too few slashes in did #{did}"
    else
      raise "too many slashes in did #{did}"
    end
    dot_sections=fnam.split(/\./)
    case dot_sections.size
    when 8 then
      dbt,dpn,daf,dbb,dvt,del,dlv,dfm=dot_sections
    when 7 then
      dlv=nil
      dbt,dpn,daf,dbb,dvt,del,dfm=dot_sections
    when 0..6 then
      raise "too few dots in did #{fnam}"
    else
      raise "too many dots in did #{fnam}"
    end
    dlv.sub!(/_/,'.') if dlv
    raise "prod name #{pn} != global" if pn != 'global'
    raise "prod name #{pn} != #{dpn}" if pn != dpn
    raise "element name #{el} != #{del}" if el != del
    raise "level name #{lv} != #{dlv}" if lv != dlv
    raise "format name #{dfm} != grib2" if dfm != 'grib2'
  end

  def run
    Dir.glob(@path).each{|gzfn|
      TarReader.open(gzfn){|tar|
	tar.each_entry{|ent|
	  next unless @rtopic === ent.name 
	  json=ent.read
	  next if json.nil?
	  rec=JSON.parse(json)
	  did = rec['properties']['data_id']
	  did_parse(did)
	}
      }
    }
  end

end

App.new(ARGV).run if $0 == __FILE__
