#!/usr/bin/ruby

require 'tarreader'
require 'json'
require 'base64'

# for wget
require 'net/http/persistent'
require 'uri'
#require 'openssl'

$LOAD_PATH.push('/var/www/html/2019/bufrconv')
require 'bufrscan'
require 'bufrdump'

class WGet

  def initialize
    @http=Net::HTTP::Persistent.new(name: 'WIS-downloader')
    @q=Queue.new
    @done=false
  end

  attr_reader :done

  def done!
    @done=true
  end

  def qdirect topic, data
    @q << [topic, data]
  end

  def quri topic, suri
    uri=URI.parse(suri)
    @q << [topic, uri]
  end

  def wget2 id
    topic=msg=nil
    topic,uri=@q.pop(true) rescue nil
    if String===uri
      msg=uri
    elsif URI===uri
      begin
        res=@http.request(uri)
        if res.code.to_i==200
          msg=res.body
        else
          STDERR.puts "#{res.code} - #{uri.path}"
        end
      rescue=>e
        STDERR.puts "#{e.class} #{e.message} - #{uri.path}"
      end
    end
    [topic,msg]
  end

  def shutdown
    @http.shutdown
  end

end

class Progress

  def initialize out
    @out=out
    @btime=Time.now.utc
    @n=0
  end

  def ping
    @n+=1
    return unless (@n % 100)==1
    t=(Time.now-@btime)
    STDERR.printf("%6u[msgs] %6.2f[s] %8.3g[msg/s]\n", @n, t, @n/t)
  end

end

class BufrCheck

  def initialize odb
    @hdr=nil
    @topic=nil
    @odb=odb
    @progress=Progress.new(STDERR)
  end

  attr_accessor :topic

  def newbufr hdr
    @hdr=hdr
  end

  def find tree, fxy
    for elem in tree
      if elem.first==fxy
        return elem[1]
      elsif Array===elem.first
        ret=find(elem.first, fxy)
        return ret if ret
      end
    end
    return nil
  end

  # practical max length = 1+1+5+1+5+1+16=30
  def wsiformat wsi1, wsi2, wsi3, wsi4
    wsi1 = if wsi1 then format('%u', wsi1) else '/' end
    wsi2 = if wsi2 then format('%u', wsi2) else '///' end
    wsi3 = if wsi3 then format('%u', wsi3) else '/' end
    wsi4 = case wsi4 when String then wsi4.rstrip when Integer then wsi4.to_s else '/////' end
    format('%-31s', [wsi1, wsi2, wsi3, wsi4].join('-'))
  end

  def utoa02 i
    if i then format('%02u', i) else '//' end
  end

  def utoa03 i
    if i then format('%03u', i) else '///' end
  end

  def subset tree
    cat=@hdr[:cat]
    subcat=@hdr[:subcat]
    srtime=@hdr[:reftime].strftime('%Y%m%dT%H%M%S')
    ii=find(tree,'001001')
    iii=find(tree,'001002')
    wsi1=find(tree,'001125')
    wsi2=find(tree,'001126')
    wsi3=find(tree,'001127')
    wsi4=find(tree,'001128')
    lat=find(tree,'005001')||find(tree,'005002')||Float::NAN
    lon=find(tree,'006001')||find(tree,'006002')||Float::NAN
    swsi=wsiformat(wsi1,wsi2,wsi3,wsi4)
    if wsi4.nil? then
      swsi=wsiformat(0,20000,0,ii*1000+iii).sub(/ /,'?')
    end
    row=[srtime,utoa02(ii)+utoa03(iii),utoa03(cat)+utoa03(subcat),
      format('%+06.2f',lat),format('%+07.2f',lon),@topic]
    @progress.ping
    if not @odb.include?(swsi) or @odb[swsi][0]<row[0] then
      @odb[swsi]=row
    end
  end

  def endbufr
    @hdr=nil
  end

  def close
  end

end

class App

  THREADS=20
  DEFPATH='/nwp/m0/jmagc[0-9][0-9].tar.gz'

  def initialize argv
    @bufrdbdir='/nwp/bin'
    @files=[]
    @gcsel='jp-jma-global-cache'
    @tpsel='/(synop|temp)$'
    for arg in argv
      @files.push arg
    end
    @files.push(DEFPATH) if @files.empty?
    @tpreg=Regexp.new(@tpsel)
    @wget=WGet.new
    @bufrdb=BufrDB.new(@bufrdbdir)
    @odb=Hash.new
    @dumper=BufrCheck.new(@odb)
    @mutex=Mutex.new
    @errs=Hash.new(0)
  end

  def fnam_to_topic topic
    topic.sub!(/\.json$/, '')
    topic.sub!(/^(wnm\d{4}-\d{6}|\d{4}[A-Z]{4})-/, '')
    topic = '(gts)' if /-gts-to-wis2_/ === topic
    topic.sub!(/_d_c_w_p_a_/, '_data_core_weather_prediction_analysis_')
    topic.sub!(/_d_c_w_p_f_/, '_data_core_weather_prediction_forecast_')
    topic.sub!(/_d_c_w_p_forecast/, '_data_core_weather_prediction_forecast')
    topic.sub!(/_d_c_w_s_sentinel/,
      '_data_core_weather_space-based-observations_sentinel')
    topic.sub!(/_d_c_w_/, '_data_core_weather_')
    topic.sub!(/_d_c_/, '_data_core_')
    topic.sub!(/_d_/, '_data_')
    topic.gsub!(/_/, '/')
    topic
  end

  def handlemsg rec, clink, topic
    if rec["content"] then
      case rec["content"]["encoding"]
      when "base64"
        @wget.qdirect(topic, Base64.decode64(rec["content"]["value"]))
        return
      end
    end
    @wget.quri(topic, clink["href"])
  end

  def readtar tarfnam
    TarReader.open(tarfnam){|tar|
      tar.each_entry{|ent|
        topic=fnam_to_topic(ent.name)
        unless @tpreg===topic
          next
        end
        json=ent.read
        if json.nil?
          @errs["nil tar entry - #{ent.name}"]+=1
          next
        end
        rec=JSON.parse(json)
        prop=rec['properties'] || Hash.new
        next if not @gcsel===prop['global-cache']
        clink=nil
        links=rec['links'] || []
        for link in links
          clink=link if /^(canonical|update)$/===link['rel']
        end
        unless clink
          @errs["missing canonical link - #{ent.name}"]+=1
          next
        end
        handlemsg(rec,clink,topic)
      }
    }
  end

  # scan tar.gz files and put URLs into @wget queue
  def phase1
    @files.each{|pat|
      Dir.glob(pat).each{|tarfnam|
        readtar(tarfnam)
      }
    }
    @wget.done!
  end

  def phase2 id
    loop do
      topic,msg=@wget.wget2(id)
      begin
        if msg.nil? or 'NIL'==msg or /\r\r\nNIL\r\r\n/===msg[0,128] then
          sleep 0.1
        elsif /BUFR/===msg[0,128]
          ofs=msg.index('BUFR')
          bmsg=BUFRMsg.new(msg,ofs,msg.size-ofs,0)
          @mutex.synchronize do
            @dumper.topic=topic
            @bufrdb.decode(bmsg,:direct,@dumper)
          end
        else
          @errs["not BUFR #{msg[0,50].inspect}"]+=1
        end
      rescue BUFRMsg::EBADF, BUFRMsg::ENOSYS => e
        @errs["#{e} - #{topic}"]+=1
      end
      break if @wget.done
    end
  end

  def compile
    producer=Thread.new {
      phase1
    }
    workers=THREADS.times.map do |id|
      Thread.new do
        phase2(id)
      end
    end
    producer.join
    workers.each(&:join)
  rescue Interrupt
    STDERR.puts "Interrupt"
  ensure
    @wget.shutdown
  end

  def run
    compile
    for wsi in @odb.keys.sort
      row=@odb[wsi]
      puts([wsi,row].flatten.join("\t"))
    end
    for msg, n in @errs
      STDERR.printf("%u: %s\n", n, msg)
    end
  end

end

App.new(ARGV).run
