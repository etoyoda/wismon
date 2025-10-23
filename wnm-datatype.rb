#!/usr/bin/ruby

require 'zlib'
require 'tarreader'
require 'json'
require 'gdbm'

require 'net/http'
require 'uri'
require 'openssl'

$LOAD_PATH.push('/var/www/html/2019/bufrconv/')
require 'bufrscan'

class WGet

  def initialize
    @host = @port = @http = nil
  end

  def connect host, port
    return true if host==@host and port==@port
    STDERR.puts "connecting #{host}:#{port}"
    @host,@port,@http=host,port,Net::HTTP.new(host,port)
    @http.use_ssl=true
    @http.verify_mode=OpenSSL::SSL::VERIFY_PEER
    true
  end

  def wget url
    uri=URI.parse(url)
    connect(uri.host, uri.port)
    req=Net::HTTP::Get.new(uri.request_uri)
    resp=@http.request(req)
    raise resp.code unless resp.code != 200
    resp.body
  end

end

SERIES = [
 ['DevGC','/nwp/m0/devgc[012][0-9].tar.gz'],
# ['DevNode','/nwp/m0/devnode[012][0-9].tar.gz'],
]

TOKYO = /(wisdev\.kishou\.go\.jp)/

gdbm = GDBM.new('/nwp/m0/mdtopic.gdbm', 0, GDBM::READER)

def guess_topic rec, gdbm
  mdid = rec['properties']['metadata_id']
  topic = if mdid then gdbm[mdid] else nil end
  if topic.nil? then
    case mdid
    when /^urn:wmo:md:([-\w]+):core\.surface-based-observations\.synop$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):upperair-weather-observations:([-\w]+)$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/#$2"
    when /^urn:wmo:md:([-\w]+):surface-weather-observations(:synop)?$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):surface-based-observations\.synop$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):surface-based-observations:temp$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/temp"
    when /^urn:wmo:md:([-\w]+):observations\.surface\.synop-bufr$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):data\.core\.weather\.surface-based-observations\.synop$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:wmo:md:([-\w]+):synop-dataset-surface-observations$/
      topic = "dummy/a/wis2/#$1/data/core/weather/surface-based-observations/synop"
    when /^urn:x-wmo:md:int-ecmwf:open-data:\d+:\d\dz:0p25:\w\w:\d+h$/
      topic = "dummy/a/wis2/int-ecmwf/data/core/weather/prediction/forecast"
    end
  end
  if topic.nil? then
    case rec['properties']['data_id']
    when /^([-\w]+)\/metadata\/urn:wmo:md/
      topic = '(metadata)'
    when /^wis2\/\w+-\w+-gts-to-wis2\//
      topic = '(gts-gw)'
    when /^weather\/surface-based-observations\/(\w+)\/A_[A-Z]{4}\d\d(\w{4})/
      topic = "(gts #$2)"
    when /^weather\/surface-based-observations\/(\w+)\/A_[A-Z]{4}\d\d(\w{4})/
    when /^origin\/a\/wis2\/([-\w]+)\/data\/(\w+)\/([-\w]+)\/([-\w]+)\/([-\w]+)/
      topic = "fake/a/wis2/#$1/data/#$2/#$3/#$4/#$5"
    when /^([-\w]+)\/data\/(core|recommended)\/(weather)\/([-\w]+)\/([-\w]+)/
      topic = "fake/a/wis2/#$1/data/#$2/#$3/#$4/#$5"
    end
  end
  topic
end

#$bufrdb=BUFRDB.new('/nwp/bin')

def bufrscan data
  msg=BUFRMsg.new(data,0,data.size,0)
  "BUFR(#{msg.descs.to_s})"
rescue BUFRMsg::ENOSYS, BUFRMsg::EBADF => e
  "BUFR(#{e.to_s})"
end

def dtype data
  case data
  when /^BUFR/ then bufrscan(data)
  when /^GRIB/ then 'GRIB'
  when /^<\?xml/ then 'XML'
  when /^[A-Z]{4}\d{2} [A-Z]{4} \d{6}( [A-Z]{3})?\s+(\w+)/ then "GTS-#{$2}"
  when /^[A-Z]{4}\d{2} [A-Z]{4} \d{6}( [A-Z]{3})?/ then 'GTS'
  else data[0,32].inspect
  end
end

ts = Hash.new()
tc = Hash.new(0)
wget = WGet.new()

SERIES.each{|name, path|
  Dir.glob(path).each{|gzfn|
    STDERR.puts "= #{gzfn}"
    TarReader.open(gzfn){|tar|
      tar.each_entry{|ent|
        json=ent.read
        rec=JSON.parse(json)
        mdid = rec['properties']['metadata_id']
        next unless mdid
        topic = guess_topic(rec, gdbm)
        case topic
        when '(gts)' then next
        when /cache\/a\/wis2\/ca-eccc-msc\/data\/core\/\w+\/experimental/ then next
        when /space-based-observations/ then next
        end
        if !topic
          if mdid then
            STDERR.puts "unresolvable mdid=#{mdid}"
          else
            STDERR.puts "unresolvable dataid=#{rec['properties']['data_id']}"
          end
          topic = '(nil)'
          next
        end
        clink = nil
        rec['links'].each{|link|
          next unless TOKYO === link['href']
          clink = link if link['rel']=='canonical'
        }
        next unless clink
        ts[topic] = Hash.new(0) unless ts.include?(topic)
        tc[topic]+=1
        x = rand()*tc[topic]
        if x > 0.5 then
          next
        end
        data = wget.wget(clink['href'])
        dt = dtype(data)
        ts[topic][dt] += 1
      }
    }
  }
}

ts.keys.sort.each do |topic|
  tab = ts[topic]
  tab.each do |dt,n|
    printf("%7u\t%s\t%s\n", n, topic, dt)
  end
end
