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
 ['DevGC','/nwp/m0/jmagc[012][0-9].tar.gz'],
]

TOKYO = /(global-cache-of-japan\.s3\.ap-northeast-1\.amazonaws\.com)/

$c=0

def counter
  printf "%06u\n", $c if ($c % 100).zero?
  $c+=1
end


def bufrtest data, ctr, stype, fnam
  msg=BUFRMsg.new(data,0,data.size,0)
  counter
rescue BUFRMsg::ENOSYS, BUFRMsg::EBADF, NoMethodError => e
  puts "BUFR(#{e.to_s}) #{ctr} #{stype}"
  fnam.sub!(/\.json$/,'.bufr')
  File.open(fnam, 'w'){|fp|
    fp.write(data)
  }
  puts "saved to #{fnam}"
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

wget = WGet.new()

SERIES.each{|name, path|
  Dir.glob(path).each{|gzfn|
    warn "= #{gzfn}"
    TarReader.open(gzfn){|tar|
      tar.each_entry{|ent|
        case ent.name
        when /([-\w]+)_data_core_weather_surface-based-observations_(\w+)/ then
          ctr, stype = $1, $2
        else next
        end
        ctr.sub!(/^\w+-/,'')
        json=ent.read
        next if json.nil?
        rec=JSON.parse(json)
        clink = nil
        rec['links'].each{|link|
          next unless TOKYO === link['href']
          clink = link if link['rel']=='canonical'
        }
        next unless clink
        data = wget.wget(clink['href'])
        bufrtest(data, ctr, stype, ent.name)
      }
    }
  }
}

