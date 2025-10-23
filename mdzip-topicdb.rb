#!/usr/bin/ruby

require 'zip'
require 'json'
require 'gdbm'

def warn str
  STDERR.puts str
end

db = GDBM.new('mdtopic.gdbm')

ARGV.each do |fnam|
  Zip::File.open(fnam) do |zipf|
    zipf.each do |ent|
      next unless /\.json$/ === ent.name
      json = ent.get_input_stream.read
      rec = JSON.parse(json)
      mdid = rec['id']
      unless mdid
        warn("missing id: " + ent.name)
        next
      end
      topic = ''
      rec['links'].each do |link|
        next unless /^mqtts?:\/\// === link['href']
        next if /\/metadata/ === link['channel']
        topic = link['channel']
      end
      topic.sub!(/^origin\/a\/wis2\//, 'cache/a/wis2/')
      if db.include?(mdid) and db[mdid] != topic then
        msg = ("overwrite(" + mdid + ") " + db[mdid] + " -> " + topic)
        if topic.empty? then
          warn("skip " + msg)
          next
        else
          warn(msg)
        end
      end
      db[mdid] = topic
    end
  end
end
db.close
