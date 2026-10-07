#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'
require 'tarwriter'
require 'net/http/persistent'
require 'uri'

class WGet

  def initialize
    @http=Net::HTTP::Persistent.new(name: 'EPT/wnm-ukwmc')
    @q=Queue.new
    @done=false
  end

  attr_reader :done

  def done!
    @done=true
  end

  def done?
    @done && @q.empty?
  end

  def submit did, entnam, uristr
    uri=URI.parse(uristr)
    @q << [did, entnam, uri]
  end

  def harvest
    did=entnam=data=nil
    begin
      did, entnam, uri=@q.pop(true)
    rescue ThreadError
      return [nil,nil,nil]
    end
    begin
      res=@http.request(uri)
      if res.code.to_i==200
        data=res.body
      else
        warn "HTTP #{res.code} #{uri}"
      end
    rescue => e
      warn "#{e.class} #{e.message} #{uri}"
    end
    [did,entnam,data]
  end

  def shutdown
    @http.shutdown
  end

end

class App

  GC_ORDER=%w(
    jp-jma-global-cache
    data-metoffice-noaa-global-cache
    cn-cma-global-cache
    kr-kma-global-cache
  )

  def initialize argv
    @path='/nwp/m0/{us,fr}gb[012][0-9].tar.gz'
    topic='uk-metoffice-wmc'
    @rtime=ENV['TIMECARD']
    @ofnam="z.ukwmc.tar"
    @otar=nil
    @invoked_at=Time.now.utc
    @dlfiles=0
    @threads=3
    for arg in argv
      case arg
      when /^--path=/ then @path = $'
      when /^--topic=/ then topic = $'
      when /^--timecard=/ then @rtime=$'
      when /^--ofnam=/ then @ofnam=$'
      when /^--threads=/ then @threads=$'.to_i
      end
    end
    @rtime=File.read('TIMECARD') if @rtime.nil?
    @rtime=Time.gm(*(@rtime.split(/ +/)))
    @rtopic=Regexp.new(topic)
    @db_did=Hash.new
    @mutex=Mutex.new
    @wget=nil
  end

  class EBADF < Errno::EBADF
  end

  ELEMS = {
  'cape-most-unstable-below-500hpa' => 'CAPEx',
  'cape-surface' => 'CAPEs',
  'dewpoint-temperature' => 'Td',
  'geopotential-height' => 'Z',
  'precipitation-accumulation-3h' => 'RRate',
  'precipitation-accumulation-6h' => 'RRate',
  'pressure-reduced-to-msl' => 'Pmsl',
  'snowfall-accumulation-water-equivalent-unsmoothed-orography-3h' => 'SnRWe',
  'snowfall-accumulation-water-equivalent-unsmoothed-orography-6h' => 'SnRWe',
  'relative-humidity' => 'RH',
  'temperature' => 'T',
  'temperature-max-3h' => 'Tmax',
  'temperature-min-3h' => 'Tmin',
  'total-cloud-cover' => 'CLA',
  'u-component-of-wind' => 'U',
  'v-component-of-wind' => 'V',
  'wind-speed-gust-max-3h' => 'maxWS',
  'wind-speed-gust-max-6h' => 'maxWS',
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
    entnam=format('%s.%s/%-7s.%04u.%03u.%-2s.grib2',
      dbt,pn,ELEMS[el],plev,ftime,AREAS[dbb]).gsub(/ /,'_')
    {
      :btime=>btime,:prodname=>pn,:elem=>ELEMS[el],:lev=>plev,
      :vtime=>vtime,:ftime=>ftime,:area=>AREAS[dbb],:entnam=>entnam
    }
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

  def wnm_register row, did, rec
    href=nil
    links=rec['links'] || []
    for link in links
      href=link['href'] if /^(canonical|update)$/===link['rel']
    end
    return unless href
    gc=rec['properties']['global-cache']
    return unless gc
    @db_did[did] = Hash.new unless @db_did.include?(did)
    row[:url] = href
    @db_did[did][gc] = row
  end

  def wnm_parse topic, json
    return unless @rtopic === topic
    return if json.nil?
    rec=JSON.parse(json)
    did = rec['properties']['data_id']
    row=did_parse(did)
    return unless filter(row)
    wnm_register(row,did,rec)
  end

  def wnm_scan
    Dir.glob(@path).each{|gzfn|
      puts "scanning #{gzfn}"
      TarReader.open(gzfn){|tar|
	tar.each_entry{|ent|
	  wnm_parse(ent.name, ent.read)
	}
      }
    }
    puts "scan elapsed #{Time.now-@invoked_at}, #{@db_did.size} dids"
  rescue Interrupt => e
  end

  def submitter gcname
    @db_did.each{|did, wnms|
      next if wnms[:done]
      next unless wnms[gcname]
      @wget.submit(did, wnms[gcname][:entnam], wnms[gcname][:url])
    }
  end

  # 重複排除は @db_dd[did][:done] の有無によるため、
  # 複数gcをスレッド並列してはならない。

  def harvester
    loop do
      did,entnam,data=@wget.harvest
      if data.nil?
        break if @wget.done?
	sleep 0.1
	next
      end
      @mutex.synchronize {
        puts "writing #{entnam} #{data.bytesize}"
	@otar.add(entnam, data)
	@db_did[did][:done]=true
	@dlfiles+=1
      }
    end
  end

  def try_gc gcname
    puts "try #{gcname}"
    @wget=WGet.new
    producer=Thread.new{ submitter(gcname) }
    workers=@threads.times.map{ |tid|
      Thread.new{ harvester }
    }
    producer.join
    @wget.done!
    workers.each(&:join)
  ensure
    @wget.shutdown if @wget
  end

  def download
    @otar=TarWriter.new(@ofnam,'w')
    GC_ORDER.each{|gc|
      try_gc(gc)
    }
  ensure
    @otar.close if @otar
  end

  def run
    wnm_scan
    download
    puts "elapsed #{Time.now-@invoked_at}, #{@dlfiles} files downloaded"
    if @dlfiles.zero?
      warn "no file downloaded"
      exit 16
    end
  end

end

App.new(ARGV).run if $0 == __FILE__
