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
    }
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
  cid,dm,pol,wx,cat=tc.split(/\//,5)
  cat='(gts)' if tc=='(gts)'
  cidstat[cid]+=1 if cid
  catstat[cat]+=1 if cat
  cat1 = if cat then cat.split(/\//,2)[0]
    else nil end
  cat2 = if cat then cat.split(/\//,3)[0,2].join('/')
    else nil end
  nstat.register(n,cat1,cat2) if cat
  zstat.register(z,cat1,cat2) if cat
end

puts "= n topics for centreid"
for cid in cidstat.keys.sort
  printf("%s\t%s\n", cid, cidstat[cid])
end

puts "= n topics for category"
for cat in catstat.keys.sort
  printf("%s\t%s\n", cat, catstat[cat])
end

nstat.report("num messages")
zstat.report("size")
