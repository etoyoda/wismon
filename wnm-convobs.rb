#!/usr/bin/ruby

require 'tarreader'
require 'json'
require 'base64'

# for wget
require 'net/http'
require 'uri'
require 'openssl'

$LOAD_PATH.push('/var/www/html/2019/bufrconv')
require 'bufrscan'
require 'bufrdump'

class WGet

  def initialize
    @host=@port=@http=nil
  end

  def connect host, port
    return true if host==@host and port==@port
    STDERR.puts "connecting #{host}:#{port}"
    @host,@port,@http=host,port,Net::HTTP.new(host,port)
    @http.use_ssl=true
    @http.verify_mode=OpenSSL::SSL::VERIFY_PEER
  end

  def wget uri
    uri=URI.parse(uri)
    connect(uri.host,uri.port)
    req=Net::HTTP::Get.new(uri.request_uri)
    resp=@http.request(req)
    raise resp.code unless resp.code != 200
    resp.body
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
    return unless 1==(@n % 100)
    t=(Time.now-@btime)
    STDERR.printf("%6u %6.2f[s] %8.2g[msg/s]\n", @n, t, @n/t)
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
    [wsi1, wsi2, wsi3, wsi4].join('-')
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
    swsi=wsiformat(0,20000,0,ii*1000+iii) if wsi4.nil?
    line=sprintf("%s\t%2s%3s\t%3s%3s\t%+06.2f\t%+07.2f\t%s\n",
      srtime, utoa02(ii), utoa03(iii),
      utoa03(cat), utoa03(subcat),
      lat, lon, @topic)
    if not @odb.include?(swsi) or @odb[swsi]<line then
      @odb[swsi]=line
    end
    @progress.ping
  end

  def endbufr
    @hdr=nil
  end

  def close
  end

end

class App

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

  def getmsg rec, clink
    if rec["content"] then
      case rec["content"]["encoding"]
      when "base64"
        return Base64.decode64(rec["content"]["value"])
      end
    end
    return @wget.wget(clink["href"])
  end

  def handlemsg rec, clink, topic
    msg=getmsg(rec,clink)
    unless /BUFR/===msg[0,128]
      raise BUFRMsg::EBADF, "not BUFR #{msg[0,32].inspect}"
    end
    bmsg=BUFRMsg.new(msg,0,msg.size,0)
    @dumper.topic=topic
    @bufrdb.decode(bmsg,:direct,@dumper)
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
          STDERR.puts "nil tar entry - #{ent.name}"
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
          STDERR.puts "missing canonical link - #{ent.name}"
          next
        end
        begin
          handlemsg(rec,clink,topic)
        rescue BUFRMsg::EBADF, BUFRMsg::ENOSYS => e
          STDERR.puts "#{e} - #{ent.name}"
        end
      }
    }
  end

  def run2
    @files.each{|pat|
      Dir.glob(pat).each{|tarfnam|
        readtar(tarfnam)
      }
    }
  rescue Interrupt
    STDERR.puts "Interrupt"
  end

  def run
    run2
    for wsi, line in @odb
      puts([wsi,line].join("\t"))
    end
  end

end

App.new(ARGV).run
