#!/usr/bin/ruby

require 'tarreader'
require 'tarwriter'
require 'json'
require 'base64'
require 'syslog'

# for wget
require 'net/http/persistent'
require 'uri'
#require 'openssl'

$facility = if STDERR.tty? then Syslog::LOG_USER else Syslog::LOG_NEWS end
Syslog.open('wnm-obscache', Syslog::LOG_PID, $facility)

def eputs msg
  Syslog.notice(msg)
  STDERR.puts(msg) if STDERR.tty?
end

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

  def done?
    @done && @q.empty?
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
          eputs "#{res.code} - #{uri.path}"
        end
      rescue=>e
        eputs "#{e.class} #{e.message} - #{uri.path}"
      end
    end
    [topic,msg]
  end

  def shutdown
    @http.shutdown
  end

end

class Progress

  def initialize
    @btime=Time.now.utc
    @n=0
  end

  def ping
    @n+=1
    return unless (@n % 100)==1
    t=(Time.now-@btime)
    STDERR.printf("%6u[msgs] %6.2f[s] %8.3g[msg/s]\n", @n, t, @n/t) if STDERR.tty?
  end

  def report
    eputs(sprintf("%6u[msgs] %6.2f[s] %8.3g[msg/s]\n", @n, t, @n/t))
  end

end

class App

  THREADS=20
  DEFPATH='/nwp/m0/jmagc[0-9][0-9].tar.gz'

  def initialize argv
    @files=[]
    @odir='/nwp/p0/incomplete'
    @gcsel='jp-jma-global-cache'
    @tpsel='(synop|temp|ship|wind-profile|buoys)'
    @opfx='wisbf'
    for arg in argv
      case arg
      when /^--gc=/ then @gcsel=$'
      when /^--topic=/ then @tpsel=$'
      when /^--odir=/ then @odir=$'
      when /^--opfx=/ then @opfx=$'
      else @files.push arg
      end
    end
    @files.push(DEFPATH) if @files.empty?
    @tpreg=Regexp.new(@tpsel)
    @wget=WGet.new
    @mutex=Mutex.new
    @progres=Progress.new
    @errs=Hash.new(0)
    ymd=File.readlink(@odir)
    raise unless /(\d\d\d\d-\d\d-\d\d)/ === ymd
    y4m2d2=$1
    if File.writable?(@odir) then
      # operational mode
      ofnam=File.join(@odir, "#{@opfx}-#{y4m2d2}.tar")
    else
      ofnam="#{@opfx}-#{y4m2d2}.tar"
    end
    @lasttime=File.stat(ofnam).mtime rescue Time.now - 7200
    @otar=TarWriter.new(ofnam,'a')
    eputs "output #{ofnam} #{ymd} #{@lasttime}"
  end

  def fnam_to_topic topic
    topic.sub!(/\.json$/, '')
    topic.sub!(/^(wnm\d{4}-\d{6}|\d{4}[A-Z]{4})-/, '')
    if /-gts-to-wis2_data_\w+_(([A-Z]_){4}\d\d)_([A-Z]{4})/ === topic
      cccc,ttaaii=$3,$1
      topic="gts-#{cccc}-#{ttaaii.gsub(/_/,'')}"
    end
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

  def handlemsg rec, clink, entname
    if rec["content"] then
      case rec["content"]["encoding"]
      when "base64"
        @wget.qdirect(entname, Base64.decode64(rec["content"]["value"]))
        return
      end
    end
    @wget.quri(entname, clink["href"])
  end

  def readtar tarfnam
    TarReader.open(tarfnam){|tar|
      tar.each_entry{|ent|
        topic=fnam_to_topic(ent.name.dup)
        unless @tpreg===topic
          @errs["skip #{topic}"]+=1 if $VERBOSE
          next
        end
        json=ent.read
        if json.nil?
          @errs["nil tar entry - #{ent.name}"]+=1
          next
        end
        rec=JSON.parse(json)
        prop=rec['properties'] || Hash.new
        if not @gcsel===prop['global-cache']
          @errs["skip gc #{prop['global-cache']}"]+=1 if $VERBOSE
          next
        end
        clink=nil
        links=rec['links'] || []
        for link in links
          clink=link if /^(canonical|update)$/===link['rel']
        end
        unless clink
          @errs["missing canonical link - #{ent.name}"]+=1
          next
        end
        handlemsg(rec,clink,ent.name)
      }
    }
  end

  # scan tar.gz files and put URLs into @wget queue
  def phase1
    @files.each{|pat|
      Dir.glob(pat).each{|tarfnam|
        tartime=File.stat(tarfnam).mtime
        if tartime < @lasttime then
          eputs "skip old #{tarfnam}"
          next
        else
          eputs "reading #{tarfnam}"
        end
        readtar(tarfnam)
      }
    }
    @wget.done!
  end

  def phase2 id
    loop do
      entname,msg=@wget.wget2(id)
      if msg.nil?
        break if @wget.done?
        sleep 0.1
        next
      end
      @mutex.synchronize do
        ofnam=entname.sub(/\.json$/,'.bin')
        eputs "writing #{ofnam}" if $VERBOSE
        @progres.ping
        @otar.add(ofnam,msg)
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
    @otar.close
    for msg, n in @errs
      eputs(sprintf("%06u: %s\n", n, msg))
    end
  end

end

App.new(ARGV).run
