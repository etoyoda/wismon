#!/usr/bin/ruby

BFTP00=/(\d{8})00\x01\r\r\n(\d\d\d)\r\r\n([A-Z]{4}\d\d [A-Z]{4} \d{6})/

msg=nil
ARGV.each{|fnam|
  File.open(fnam, 'r:ASCII-8BIT'){|fp|
    msg=fp.read
  }
  pos=0
  while BFTP00===msg[pos,128] do
    blen,nnn,hdr=$1.to_i,$2,$3
    printf("%08u BATCH seq=%s size=%08u hdr=%s\n", pos, nnn, blen, hdr)
    bulletin=msg[pos+10,blen]
    if /BUFR/===bulletin
      ofs=bulletin.index('BUFR')
      sz=("\x00"+bulletin[ofs+4,3]).unpack('N').first
      ed=bulletin[ofs+7].unpack('C').first
      gap=blen-ofs-sz
      gap-=4 #for \r\r\n\x03
      printf("%08u BUFR ofs=%04u ed=%u sz=%08u gap=%d\n", pos+10+ofs, ofs, ed, sz, gap)
      bufrtail=bulletin[ofs+sz-4,4]
      unless "7777"==bufrtail then
        printf("%08u ENDBUFR %s - should be 7777\n", pos+10+ofs+sz-4, bufrtail.inspect)
      end
      bulltail=bulletin[ofs+sz..-1]
      unless "\r\r\n\x03"==bulltail then
        printf("%08u ENDBULL %s - should be \\r\\r\\n\\x03\n", pos+10+ofs+sz, bulltail.inspect)
      end
    end
    pos+=(blen+10)
  end
  remain = msg[pos,msg.bytesize-pos]
  if '0000000000' == remain then
    puts "BATCH END"
  else
    printf("BATCH END Missing: pos=%06u filesize=%06u\n", pos, msg.bytesize)
  end

}
