#!/usr/bin/ruby

cidstat=Hash.new(0)
catstat=Hash.new(0)
cat1nstat=Hash.new(0)
cat1zstat=Hash.new(0)

for line in ARGF
  n,z,tc=line.strip.split(/,/,3)
  n=n.to_i
  z=z.to_f
  cid,dm,pol,wx,cat=tc.split(/\//,5)
  cidstat[cid]+=1 if cid
  catstat[cat]+=1 if cat
  cat1 = if cat then cat.split(/\//,2)[0]
    elsif tc=='(gts)' then '(gts)'
    else nil
    end
  cat1nstat[cat1]+=n if cat1
  cat1zstat[cat1]+=z if cat1 and not z.nan?
end

puts "= centre_id, #topics for"
for cid in cidstat.keys.sort
  printf("%s\t%s\n", cid, cidstat[cid])
end

puts "= category, #topics for"
for cat in catstat.keys.sort
  printf("%s\t%s\n", cat, catstat[cat])
end

puts "= category1, #messages for"
sum=0
cat1nstat.values.each{|n| sum+=n}
for cat in cat1nstat.keys.sort{|a,b|cat1nstat[b]<=>cat1nstat[a]}
  printf("%7u\t%6.1f\t%s\n", cat1nstat[cat], cat1nstat[cat]*100.0/sum, cat)
end



