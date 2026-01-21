#!/usr/bin/ruby

lhs=Hash.new

lfnam=ARGV.shift
rfnam=ARGV.shift

File.open(lfnam){|lfp|
  lfp.each{|line|
    hdr=line.chomp.split(/,/,2).first
    lhs[hdr]=line
  }
}

File.open(rfnam){|rfp|
  rfp.each{|line|
    hdr=line.chomp.split(/,/,2).first
    if lhs.include?(hdr) then
      lhs.delete(hdr)
    else
      puts "> #{line}"
    end
  }
}

lhs.each{|hdr,line|
      puts "< #{line}"
}
