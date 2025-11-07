#!/usr/bin/ruby

require 'tarreader'
require 'json'
require 'base64'

require 'net/http'
require 'uri'
require 'openssl'
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

class App

  DEFPATH='/nwp/m0/jmagc[0-9][0-9].tar.gz'

  def initialize argv
    @files=[]
    @gcsel='jp-jma-global-cache'
    @tpsel='/(synop|temp)$'
    for arg in argv
      @files.push arg
    end
    @files.push(DEFPATH) if @files.empty?
    @tpreg=Regexp.new(@tpsel)
    @wget=WGet.new
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

  def handlemsg rec,clink,topic
    msg=getmsg(rec,clink)
    p msg[0,4] if msg
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
        handlemsg(rec,clink,topic)
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
  end

end

App.new(ARGV).run
