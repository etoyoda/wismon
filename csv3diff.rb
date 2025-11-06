#!/usr/bin/ruby

unless 2 == ARGV.size
  puts "usage: #$0 file1.csv file2.csv"
  puts "\textracts column 3 and take diff"
  exit 16
end

db=Hash.new(0)

fnam1,fnam2=ARGV
File.open(fnam1) {|fp|
  fp.each_line {|line|
    row=line.chomp.split(/,/,4)
    db[row[2]]+=1
  }
}
File.open(fnam2) {|fp|
  fp.each_line {|line|
    row=line.chomp.split(/,/,4)
    db[row[2]]+=2
  }
}

puts "= only in #{fnam1}"
diff1=Hash.new
db.each{|c3,val|
  next unless val==1
  diff1[c3]=true
}
diff1.keys.sort.each{|c3| puts c3}

puts "= only in #{fnam2}"
diff2=Hash.new
db.each{|c3,val|
  next unless val==2
  diff2[c3]=true
}
diff2.keys.sort.each{|c3| puts c3}

