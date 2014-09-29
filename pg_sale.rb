#!/usr/bin/ruby
start = Time.now

require 'yaml'


def require_library_or_gem(library_name)
  begin
    require library_name
  rescue LoadError => cannot_require
    # 1. Requiring the module is unsuccessful, maybe it's a gem and nobody required rubygems yet. Try.
    begin
      require 'rubygems'
    rescue LoadError => rubygems_not_installed
      raise cannot_require
    end
    # 2. Rubygems is installed and loaded. Try to load the library again
    begin
      require library_name
    rescue LoadError => gem_not_installed
      raise cannot_require
    end
  end
end


begin
  require_library_or_gem 'pg'
rescue LoadError => e
  begin
    require_library_or_gem 'postgres'
    class PGresult
      alias_method :nfields, :num_fields unless self.method_defined?(:nfields)
      alias_method :ntuples, :num_tuples unless self.method_defined?(:ntuples)
      alias_method :ftype, :type unless self.method_defined?(:ftype)
      alias_method :cmd_tuples, :cmdtuples unless self.method_defined?(:cmd_tuples)
    end
  rescue LoadError
    raise e
  end
end





class ::Hash

  # Add recursively value in a Hash
  def rstore(key, value)
    if key.to_s.match(/\./)
      levels = key.split(/\./)
      root = levels[0].to_sym
      self[root] = {} unless self[root].is_a? Hash
      self[root].rstore(levels[1..-1].join("."), value)
    else
      self[key.to_sym] = value
    end
  end

end

# Convert ARGV style array in a recursive_hash
def argv_to_hash(argv)
  args = {}
  for arg in argv
    a = arg.split(/\=/)
    args.rstore(a[0], a[1..-1].join("="))
  end
  return args
end


def check_equals(variable, expected, message)
  if variable != expected
    puts "ERROR: #{message} (#{expected.inspect} expected, #{variable.inspect} got)"
    exit 0
  end
end

class TaskPostgres
  
  def initialize(conn={})
    @debug = conn.delete :debug
    conn[:user] ||= conn[:username]||"postgres"
    conn[:dbname] ||= "postgres"
    @conn = PGconn.open(conn)
    @types = {}
    @conn.exec('SELECT oid, typname from pg_type where typrelid=0 and typelem=0').each do |t|
      @types[t['oid']] = t['typname'].to_sym
    end
  end

  def log(*args)
    puts(args[0].gsub(/^/, "B> "), *args[1..-1]) if @debug
  end

  def select(query)
    log query.to_s
    result = @conn.exec(query)
    columns = {}
    count = result.ntuples
    result.fields.size.times do |i|
      name = result.fields[i]
      columns[name.gsub(/[^\w]/i, '_').to_sym] = {:type=>@types[result.ftype(i).to_s], :name=>name.to_s, :index=>i}
    end
    # log "Query count: "+count.to_s
    return result, columns
  end

  def exec(query)
    log query.to_s
    return @conn.exec(query)
  end


  def quote(string, type)
    return "NULL" if string.to_s.strip.size.zero?
    case type
    when :bool then (['f', 'false', '0', 'null', 'n', 'no'].include?(string.to_s.downcase) ? 'FALSE' : 'TRUE')
    when :int2, :int4, :int8, :numeric, :float4, :float8 then string
    when :date then "'#{string}'"
    when :timestamp, :time_stamp then "'#{string.gsub(/[a-zA-Z]/, ' ')}'"
    when :varchar, :string then "'"+string.gsub("'", "''")+"'"
    else
      raise "Unknown type: #{type.inspect}"
    end
  end

  def unquote(string, type)
    return nil if string.nil?
    case type
    when :bool then (['f', 'false', '0', 'null', 'n', 'no'].include?(string.downcase) ? false : true)
    when :int2, :int4, :int8 then string.to_i
    when :numeric then string.to_d
    when :float4, :float8 then string.to_f
    when :date then Date.civil(*string.split(/[^0-9]+/).collect{|x| x.to_i})
    when :timestamp, :time_stamp then Time.gm(*string.split(/[^0-9]+/)) # "'#{string.gsub(/[a-zA-Z]/, ' ')}'"
    when :varchar, :string then string
    else
      raise "Unknown type: #{type.inspect}"
    end
  end

end


class PgSale < TaskPostgres

  def get(table, search, attributes)
    r, columns = self.select("SELECT #{table}.* FROM #{table} LIMIT 0")
    conditions = search.collect{|k,v| "#{k}=#{quote(v, columns[k][:type])}" }.join(" AND ")
    result = self.exec("SELECT id FROM #{table} WHERE "+conditions)
    values, id = search.merge(attributes), nil
    values.each{ |k,v| check_equals(columns[k].class, Hash, "Unknown column #{k}") }
    if result.count > 0
      id = result.getvalue(0,0)
      self.exec("UPDATE #{table} SET "+values.collect{|k,v| "#{k}=#{quote(v, columns[k][:type])}"}.join(', ')+", updated_at=CURRENT_TIMESTAMP WHERE "+conditions)
    else
      cols = values.keys
      id = self.exec("INSERT INTO #{table} (#{cols.join(', ')}, created_at, updated_at) VALUES ("+cols.collect{|c| quote(values[c], columns[c][:type])}.join(', ')+", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id").getvalue(0,0)
    end
    record = {}
    self.exec("SELECT * FROM #{table} WHERE id=#{id}")[0].each{|k,v| record[k.to_sym] = unquote(v, columns[k.to_sym][:type])}
    return record
  end


  def get_entity(pkc, pkid, entity)
    contact = entity.delete(:contact)

    nature = entity.delete(:nature)
    nature[:title] ||= nature[:name]
    nature = get(:entity_natures, {:name=>nature[:name], :company_id=>entity[:company_id]}, nature)
    
    entity[:nature_id] = nature[:id]
    entity[:language] ||= "fra"
    entity[:category_id] ||= self.exec("SELECT id FROM entity_categories WHERE company_id=#{entity[:company_id]} ORDER BY id").getvalue(0,0).to_i
    entity[:full_name] = (entity[:last_name].to_s+" "+entity[:first_name].to_s).strip
    entity[:full_name] = (nature[:title].to_s+" "+entity[:full_name]).strip unless nature[:in_name]
    entity = get(:entities, {pkc=>pkid, :company_id=>entity[:company_id]}, entity)

    contact[:company_id] = entity[:company_id]
    contact = get(:contacts, {:entity_id=>entity[:id], :by_default=>true}, contact)
    return entity
  end

end

argv = argv_to_hash(ARGV)
# puts argv.inspect
check_equals(argv[:dbconf].class, String, "Needs 'dbconf=/path/to/database.yml' parameter")
dbc = YAML.load_file(argv[:dbconf])[argv[:env]||"production"]
pgs = PgSale.new(:user=>dbc["username"], :password=>dbc["password"], :dbname=>dbc["database"], :debug=>(argv[:debug].to_i>0 ? true : false))

check_equals(argv[:client].class, Hash, "Needs 'entity.YYY=XXX' parameters")
check_equals(argv[:client][:code].class, String, "Needs 'entity.code=XXX' parameter")
check_equals(argv[:client][:last_name].class, String, "Needs 'entity.last_name=XXX' parameter")
check_equals(argv[:client][:company_id].class, String, "Needs 'entity.company_id=XXX' parameter")
check_equals(argv[:client][:nature].class, Hash, "Needs 'entity.nature.YYY=XXX' parameters")
check_equals(argv[:client][:nature][:name].class, String, "Needs 'entity.nature.name=XXX' parameter")
check_equals(argv[:client][:contact].class, Hash, "Needs 'entity.contact.YYY=XXX' parameters")

# Find or create
uid = (argv[:uid] || :external_id).to_sym
check_equals(argv[:client][uid].class, String, "Needs 'entity.#{uid}=XXX' parameter")

c = pgs.get_entity(uid, argv[:client][uid], argv[:client])

# Open web browser
url = (argv[:url] || "http://localhost:3000").gsub(/[\/\\]*$/, '')
url = "http://"+url unless url.match(/\:\/\//)
url += "/management/sales_order_create?client_id=#{c[:id]}"
pgs.log "URL: "+url
unless argv[:open] == "0"
  if RUBY_PLATFORM.match(/linux/)
    system("firefox #{url}")
  else
    system("start #{url}")
  end
end

# Display time
pgs.log "#{Time.now - start} seconds"
