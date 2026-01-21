#!/usr/bin/ruby
require 'mqtt'
require 'json'
require 'timeout'
require 'tarwriter'
require 'uri'

$cfg = Hash.new
$topics = Array.new

def setcfg key
  json=JSON.parse(File.read('/usr/local/etc/wismon-cfg.json'))
  raise "unresolvable #{key}" unless json[key]
  u=URI.parse(json[key])
  $cfg[:host] = u.host
  $cfg[:port] = u.port || 8883
  $cfg[:ssl] = true
  $cfg[:username] = u.user
  $cfg[:password] = u.password
  t = u.path.sub(/^\//, '')
  t += "##{u.fragment}" if t and u.fragment
  $topics.push t unless t.empty?
end

for arg in ARGV
  case arg 
  when /^(-h|--host=)/ then $cfg[:host] = $'
  when /^(-p|--port=)/ then $cfg[:port] = $'
  when /^(-u|--user=)/ then $cfg[:username] = $'
  when /^(-s|--pass=)/ then $cfg[:password] = $'
  when /^(-o|--out=)/ then fnpat = $'
  else
    if $cfg[:host].nil? then
      setcfg(arg)
    else
      $topics.push arg
    end
  end
end

class TarSaver

  def initialize fnpat
    @fnpat = fnpat
    @fnam = nil
    @tar = nil
    @n = nil
    @now = nil
    clear
  end

  def mkfnam ofs=0
    @now = Time.now.utc
    @now += ofs
    @now.strftime(@fnpat)
  end

  def clear
    tmp = mkfnam(-3600)
    if File.exist?(tmp) then
      STDERR.puts "gzip -f #{tmp}"
      system "gzip -f #{tmp}"
    end
  end

  def save
    tmp = mkfnam()
    if @fnam != tmp then
      if @tar then
        @tar.close
        STDERR.puts "gzip -f #{@fnam}"
        system "gzip -f #{@fnam}"
      end
      @fnam = tmp
      @tar = TarWriter.new(@fnam, 'a')
      STDERR.puts "open #{@fnam}"
      @n = 'AAAA'
    end
    yield @tar, @n, @now
    @n = @n.succ
  end

  def close
    @tar.close if @tar
    @tar = @fnam = nil
  end

end

MAXFNAM=85

ss = TarSaver.new(fnpat)
begin
  MQTT::Client.connect($cfg) do |c|
    STDERR.puts "connected #{$cfg[:host]}:#{$cfg[:port]}"
    for topic in $topics
      c.subscribe(topic)
      STDERR.puts "subscribed #{topic}"
    end
    c.get do |topic,message|
      ss.save { |tar, n, now|
        tnam = topic.sub(/^\w+\/a\/wis2\//,'')
        tnam.sub!(/(\/\w)[-\w]+/, '\1') or break while tnam.length > MAXFNAM
        tnam = tnam[0,MAXFNAM] if tnam.length > MAXFNAM
        tnam.gsub!(/\//, '_')
        mnam = "#{now.strftime('%d%H')}#{n}-#{tnam}.json"
        STDERR.puts("save #{topic} #{mnam}") if $VERBOSE
        tar.add(mnam, message)
      }
    end
  end
rescue MQTT::ProtocolException
  retry
rescue Timeout::Error
  retry
rescue SignalException => e
  ss.close
  STDERR.puts "#{e} - quitting"
  exit 16
end
