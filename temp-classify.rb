#!/usr/bin/ruby

db=Hash.new
for line in ARGF
  row=line.chomp.split(/\t/)
  next unless /gts-IU[SK]|\/temp$/===row[7]
  centre=row[7].sub(/\/.*/,'')
  centre=$1 if /^gts-[A-Z]{4}\d\d-([A-Z]{4})/=== centre
  type=nil
  case row[0]
  when /^0-0-0- *$/ then next
  when /^0-20000-0-\d+! *$/ then type='WSI present 0-20000-0-*'
  when /^0-20001-0-\d+ *$/ then type="WSI present 0-20001-0-*"
  when /^0-20001-0-\d+\? *$/ then type='WSI missing'
  when /^0-(\d+)-(\d+)-\w+ *$/ then type="WSI present 0-#$1-#$2-*"
  else raise row[0]
  end
  db[type]=Hash.new(0) unless db.include?(type)
  db[type][centre]+=1
end

db.keys.sort.each{|type|
  puts "#{type}:"
  puts " #{db[type].keys.sort.join(' ')}"
}
