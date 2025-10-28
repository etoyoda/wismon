#!/usr/bin/ruby

class Stat

  def initialize
    @stat1=Hash.new(0)
    @stat2=Hash.new(0)
    @sum=0.0
  end

  def register metric,cat1,cat2
    return false if metric.nan?
    @stat1[cat1]+=metric
    @stat2[cat2]+=metric
    @sum+=metric
  end

  def format7 n
    if n<999.5 then format("%6.3f",n)
      elsif n<999.5e3 then format("%6.2fk",n*0.001)
      elsif n<999.5e6 then format("%6.2fM",n*1.0e-6)
      elsif n<999.5e9 then format("%6.2fG",n*1.0e-9)
      else format("%6.2fT",n*1.0e-12)
      end
  end

  def report kwd
    puts "= "+kwd
    @stat1.keys.sort{|a,b| @stat1[b]<=>@stat1[a] }.each{|cat1|
      n=@stat1[cat1]
      r=100.0*n/@sum
      printf "%7s\t%5.2f%%\t%s\n", format7(n), r, cat1
      list2=@stat2.keys.select{|cat2|
          cat2.start_with?(cat1)
        }.sort{|a,b|
          @stat2[b]<=>@stat2[a]
        }
      if list2.size>1 or list2.first!=cat1 then
        list2.each do |cat2|
          n=@stat2[cat2]
          r=100.0*n/@sum
          c=cat2[cat1.length..-1]
          c='.' if c.empty?
          printf "\t\t%7s\t%5.2f%%\t%s\n", format7(n), r, c
        end
      end
    }
    printf "%7s\t100.0%%\t(total)\n", format7(@sum)
  end

end

cidstat=Hash.new(0)
catstat=Hash.new(0)
nstat=Stat.new
zstat=Stat.new

for line in ARGF
  n,z,tc=line.strip.split(/,/,3)
  n=n.to_f
  z=z.to_f*n*1000.0
  cid,ntyp,dpol,esd=tc.split(/\//,4)
  esd='(gts)' if tc=='(gts)'
  cidstat[cid]+=1 if cid
  catstat[esd]+=1 if esd
  if esd
    cat1 = cat2 = nil
    cat1 = esd.split(/\//,3)[0,2].join('/')
    cat2 = esd.split(/\//,4)[0,3].join('/')
    nstat.register(n,cat1,cat2)
    zstat.register(z,cat1,cat2)
  end
end

nstat.report("num messages")
zstat.report("size")

puts "= n topics for centreid"
for cid in cidstat.keys.sort
  printf("%s\t%s\n", cid, cidstat[cid])
end

puts "= n topics for category"
for esd in catstat.keys.sort
  printf("%s\t%s\n", esd, catstat[esd])
end
