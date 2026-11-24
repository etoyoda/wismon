#!/usr/bin/ruby
db=Hash.new
for line in ARGF
  row=line.chomp.split(/\t/,9)
  wsi=row.first
  if not db.include?(wsi) then
    db[wsi]=row
  else
    old=db[wsi]
    first=[old[2],row[2]].min
    if old[1]<row[1] then
      db[wsi]=row
    end
    db[wsi][2]=first
  end
end
db.keys.sort.each{|wsi|
  puts(db[wsi].join("\t"))
}
