#!/usr/bin/ruby
db=Hash.new
for line in ARGF
  row=line.chomp.split(/\t/,9)
  wsi=row.first
  if not db.include?(wsi) then
    db[wsi]=row
  elsif db[wsi][1]<row[1] then
    row[2]=db[wsi][2]
    db[wsi]=row
  end
end
db.keys.sort.each{|wsi|
  puts(db[wsi].join("\t"))
}
