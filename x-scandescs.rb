#!/usr/bin/ruby
require '/nwp/bin/bufrdump'

class App

  def initialize
    @bufrdbdir='/nwp/share/bufrconv'
    @bufrdb=BufrDB.new(@bufrdbdir)
  end

  WISHES=%w( 001001 001002 001011 001125 001126 001127 001128
  005001 005002 006001 006002
  001087 001003 001020 001005 001015 002011 001101 001102 )

  def checkdescs sdescs
    descs=@bufrdb.compile(sdescs.split(/[ ,]/))
    bitofs=0
    actions=[]
    descs.each{|desc|
      if WISHES.include?(desc[:fxy])
        actions.push([bitofs, desc])
      end
      break unless desc[:width]
      bitofs+=desc[:width]
    }
    # show
    actions.each{|ibit, desc|
      printf("%3u,%6s,%3u,%3d,%9d,%10s,%40s\n",
             ibit, desc[:fxy], desc[:width], desc[:scale], desc[:refv], desc[:units], desc[:desc])
      }
  end

  def run argv
    argv=['301150,307096'] if argv.empty?
    for sdescs in argv
      checkdescs(sdescs)
    end
  end

end

App.new.run(ARGV) if $0 == __FILE__
