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
  end

  def mkfnam
    @now = Time.now.utc
    @now.strftime(@fnpat)
  end

  def save
    tmp = mkfnam()
    if @fnam != tmp then
      if @tar then
        @tar.close
        system "gzip -f #{@fnam}"
      end
      @fnam = tmp
      @tar = TarWriter.new(@fnam, 'a')
      STDERR.puts "open #{@fnam}"
      @n = '000000'
    end
    yield @tar, @n, @now
    @n = @n.succ
  end

  def close
    @tar.close if @tar
    @tar = @fnam = nil
  end

end


ss = TarSaver.new(fnpat)
begin
  MQTT::Client.connect($cfg) do |c|
    STDERR.puts "connected #{$cfg[:host]}"
    for topic in $topics
      c.subscribe(topic)
      STDERR.puts "subscribed #{topic}"
    end
    c.get do |topic,message|
      ss.save { |tar, n, now|
        tnam = topic.sub(/^\w+\/a\/wis2\//,'')
        tnam.sub!(/(\/\w)[-\w]+/, '\1') or break while tnam.length > 64
        tnam = tnam[0,64] if tnam.length > 64
        tnam.gsub!(/\//, '_')
        mnam = "wnm#{now.strftime('%d%H')}-#{n}-#{tnam}.json"
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
