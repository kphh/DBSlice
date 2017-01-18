class DatabaseSlice

  def initialize(skips = [])
    @dump = Hash.new(Set.new)
    @skips = skips
    @config = YAML.load_file("#{Rails.root}/lib/db_slice/config.yml")
  end

  def pull(model, model_ids)
    ActiveRecord::Base.establish_connection(@config["source"])

    models = model.where(model.primary_key => model_ids)
    @dump[model.table_name] += models
    children = (model.reflect_on_all_associations(:has_many) + model.reflect_on_all_associations(:has_one)).reject { |c| c.is_a? ActiveRecord::Reflection::ThroughReflection }
    children.each do |child|
      step_down(child, model_ids)
    end
    step_up(models)

    ActiveRecord::Base.establish_connection(Rails.env)
  end

  def insert_and_sanitize(batch_size = 50)
    ActiveRecord::Base.establish_connection(@config["destination"])
    ActiveRecord::Base.connection.execute("SET FOREIGN_KEY_CHECKS = 0")
    @dump.each do |table, records|
      next if records.empty?
      columns = records.first.class.columns.map(&:name)
      backticked_columns = columns.map { |c| "`#{c}`" }
      records.each_slice(batch_size) do |batch|
        values = values_string(batch, columns)
        sql = "INSERT INTO `#{table}` (#{backticked_columns.join(", ")}) VALUES #{values} ON DUPLICATE KEY UPDATE "
        sql += (backticked_columns - ["`#{batch.first.class.primary_key}`"]).map { |column| "#{column} = VALUES(#{column})" }.join(", ")
        ActiveRecord::Base.connection.execute(sql)
      end
    end
    sanitize
    ActiveRecord::Base.connection.execute("SET FOREIGN_KEY_CHECKS = 1")
    ActiveRecord::Base.establish_connection(Rails.env)
  end

  private

  def step_up(models)
    assocs = models.first.class.reflect_on_all_associations(:belongs_to)
    polymorphic_assocs = assocs.select { |a| a.options[:polymorphic] }
    non_polymorphic_assocs = assocs - polymorphic_assocs

    non_polymorphic_assocs.each do |assoc|
      klass = assoc.klass
      @dump[klass.table_name] += klass.where(klass.primary_key => models.map { |m| m.send(assoc.foreign_key) }.uniq.compact) unless @skips.include?(klass)
    end

    polymorphic_ids = Hash.new(Set.new)

    polymorphic_assocs.each do |assoc|
      models.each do |model|
        klass = model.send(assoc.foreign_type)
        begin
          polymorphic_ids[klass.constantize] << model.send(assoc.foreign_key) if klass.present? && !@skips.include?(klass.constantize)
        rescue NameError => e
          next
        end
      end
    end

    polymorphic_ids.each do |model, ids|
      @dump[model.table_name] += model.where(model.primary_key => ids)
    end
  end

  def step_down(assoc, parent_ids)
    model = assoc.klass
    return if @skips.include? model
    models = model.where(assoc.foreign_key => parent_ids)
    # can't merge these in because sometimes conditions are a hash, sometimes a string
    models = models.where(assoc.options[:conditions]) if assoc.options[:conditions].present?
    model_ids = models.map(&model.primary_key.to_sym)
    # check against model_ids instead of models because that fires a COUNT() query
    return unless model_ids.present?
    @dump[model.table_name] += models

    child_assocs = (model.reflect_on_all_associations(:has_many) + model.reflect_on_all_associations(:has_one)).reject { |c| c.is_a? ActiveRecord::Reflection::ThroughReflection }

    child_assocs.each do |assoc|
      step_down(assoc, model_ids.uniq.compact)
    end

    step_up(models)
  end

  def values_string(records, columns)
    "(" +
      records
        .map(&:attributes)
        .map { |attrs| attrs.select { |column, _val| columns.include? column } }
        .map(&:values)
        .map do |attrs|
          attrs.map do |value|
            escaped_value = value.to_s.gsub("'", %q(\\\'))
            value.nil? ? "NULL" : "'#{escaped_value}'"
          end
        end
        .map { |attrs| attrs.join(", ") }
        .join("), (") +
    ")"
  end

  def sanitize
    File.read("#{Rails.root}/lib/db_slice/sanitize.sql")
      .split("\n")
      .select { |sql| sql.present? && sql[0..3] != "drop" }
      .each { |sql| ActiveRecord::Base.connection.execute(sql) }
  end
end
