#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'

class App

  def initialize argv
    @path='/nwp/m0/{us,fr}gb[012][0-9].tar.gz'
    topic='uk-metoffice-wmc'
    @rtime=ENV['TIMECARD']
    for arg in argv
      case arg
      when /^--path=/ then @path = $'
      when /^--topic=/ then topic = $'
      when /^--timecard=/ then @rtime=$'
      end
    end
    @rtime=File.read('TIMECARD') if @rtime.nil?
    @rtime=Time.gm(*(@rtime.split(/ +/)))
    @rtopic=Regexp.new(topic)
  end

  class EBADF < Errno::EBADF
  end

  ELEMS = {
  'cape-most-unstable-below-500hpa' => 'CAPEx',
  'cape-surface' => 'CAPEs',
  'dewpoint-temperature' => 'Td',
  'geopotential-height' => 'Z',
  'precipitation-accumulation-3h' => 'RAIN',
  'precipitation-accumulation-6h' => 'RAIN',
  'pressure-reduced-to-msl' => 'Pmsl',
  'snowfall-accumulation-water-equivalent-unsmoothed-orography-3h' => 'SNOW',
  'snowfall-accumulation-water-equivalent-unsmoothed-orography-6h' => 'SNOW',
  'relative-humidity' => 'RH',
  'temperature' => 'T',
  'temperature-max-3h' => 'Tmax',
  'temperature-min-3h' => 'Tmin',
  'total-cloud-cover' => 'CLA',
  'u-component-of-wind' => 'U',
  'v-component-of-wind' => 'V',
  'wind-speed-gust-max-3h' => 'GUST',
  'wind-speed-gust-max-6h' => 'GUST',
  }

  AREAS = {
    '-170_-90_-50_0' => 'SW',
    '-50_-90_70_0' => 'SC',
    '70_-90_-170_0' => 'SE',
    '-170_0_-50_90' => 'NW',
    '-50_0_70_90' => 'NC',
    '70_0_-170_90' => 'NE'
  }

  def did_parse did
    sl_sections=did.split(/\//)
    case sl_sections.size
    when 7 then
      pn,el,af,lv,ft,fnam,stime=sl_sections
    when 6 then
      lv=nil
      pn,el,af,ft,fnam,stime=sl_sections
    when 0..5 then
      raise EBADF, "too few slashes in did : #{did}"
    else
      raise EBADF, "too many slashes in did : #{did}"
    end
    dot_sections=fnam.split(/\./)
    case dot_sections.size
    when 8 then
      dbt,dpn,daf,dbb,dvt,del,dlv,dfm=dot_sections
    when 7 then
      dlv=nil
      dbt,dpn,daf,dbb,dvt,del,dfm=dot_sections
    when 0..6 then
      raise EBADF, "too few dots in did : #{fnam}"
    else
      raise EBADF, "too many dots in did : #{fnam}"
    end
    dlv.sub!(/_/,'.') if dlv
    raise EBADF, "prod name #{pn} != global" if pn != 'global'
    raise EBADF, "prod name #{pn} != #{dpn}" if pn != dpn
    raise EBADF, "element name #{el} != #{del}" if el != del
    raise EBADF, "unknown element #{el}" unless ELEMS[el]
    raise EBADF, "unknown area #{dbb}" unless AREAS[dbb]
    raise EBADF, "level name #{lv} != #{dlv}" if lv != dlv
    plev = case lv
      when '1.5','10',nil then 1013
      when /^\d+$/ then lv.to_i / 100
      else raise EBADF, "unknown level #{lv}"
      end
    raise EBADF, "format name #{dfm} != grib2" if dfm != 'grib2'
    unless /^(\d{4})(\d\d)(\d\d)T(\d\d)(\d\d)(\d\d)Z$/ =~ dbt
      raise EBADF, "bad btime #{dbt}"
    end
    btime=Time.gm($1,$2,$3,$4,$5,$6)
    unless /^T(\+\d+)$/ =~ ft
      raise EBADF, "bad ftime #{ft}"
    end
    ftime = $1.to_i
    unless /^(\d{4})-(\d\d)-(\d\d)T(\d\d)_(\d\d)_(\d\d)Z$/ =~ dvt
      raise EBADF, "bad vtime #{dvt}"
    end
    vtime=Time.gm($1,$2,$3,$4,$5,$6)
    unless btime + 3600 * ftime == vtime
      raise EBADF, "inconsistent ftime #{btime} #{ft} #{dvt}"
    end
    case daf
    when 'forecast' then
      raise EBADF, "unknown af #{af}" if af != daf
      raise EBADF, "forecast ft=0" if ftime.zero?
    when 'analysis' then
      raise EBADF, "analysis ft=#{ftime}" if ftime > 0
      raise EBADF, "unknown af #{af}" if /^(analysis|forecast)$/ !~ af
    else
      raise EBADF, "unknown daf #{daf}"
    end
    {:btime=>btime,:prodname=>pn,:elem=>ELEMS[el],:lev=>plev,
    :vtime=>vtime,:ftime=>ftime,:area=>AREAS[dbb]}
  end

  def filter row
    return false if @rtime != row[:btime]
    if [6,9,15].include?(row[:ftime])
      case row[:lev]
      when 1013
        ['U','V','T','Pmsl','RAIN'].include?(row[:elem])
      when 250,500,700,850
        ['U','V','T','RH','Z'].include?(row[:elem])
      end
    else false
    end
  end

  def run3 topic, json
    return unless @rtopic === topic
    return if json.nil?
    rec=JSON.parse(json)
    did = rec['properties']['data_id']
    row=did_parse(did)
    return unless filter(row)
    @z[row[:btime]]+=1
  end

  def run2
    Dir.glob(@path).each{|gzfn|
      TarReader.open(gzfn){|tar|
	tar.each_entry{|ent|
	  run3(ent.name, ent.read)
	}
      }
    }
  rescue Interrupt => e
  end

  def run
    @z=Hash.new(0)
    run2
    puts @z.inspect
  end

end

App.new(ARGV).run if $0 == __FILE__
