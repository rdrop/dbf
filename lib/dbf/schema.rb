module DBF
  # The Schema module is mixin for the Table class
  module Schema
    FORMATS = [:activerecord, :json, :sequel, :snowflake].freeze

    OTHER_DATA_TYPES = {
      'Y' => ':decimal, :precision => 15, :scale => 4',
      'D' => ':date',
      'T' => ':datetime',
      'L' => ':boolean',
      'M' => ':text',
      'B' => ':binary'
    }.freeze

    # Generate an ActiveRecord::Schema
    #
    # xBase data types are converted to generic types as follows:
    # - Number columns with no decimals are converted to :integer
    # - Number columns with decimals are converted to :float
    # - Date columns are converted to :datetime
    # - Logical columns are converted to :boolean
    # - Memo columns are converted to :text
    # - Character columns are converted to :string and the :limit option is set
    #   to the length of the character column
    #
    # Example:
    #   create_table "mydata" do |t|
    #     t.column :name, :string, :limit => 30
    #     t.column :last_update, :datetime
    #     t.column :is_active, :boolean
    #     t.column :age, :integer
    #     t.column :notes, :text
    #   end
    #
    # @param format [Symbol] format Valid options are :activerecord and :json
    # @param table_only [Boolean]
    # @return [String]
    def schema(format = :activerecord, table_only = false)
      schema_method_name = schema_name(format)
      send(schema_method_name, table_only)
    rescue NameError
      raise ArgumentError, ":#{format} is not a valid schema. Valid schemas are: #{FORMATS.join(', ')}."
    end

    def schema_name(format) # :nodoc:
      "#{format}_schema"
    end

    def activerecord_schema(_table_only = false) # :nodoc:
      s = "ActiveRecord::Schema.define do\n"
      s << "  create_table \"#{name}\" do |t|\n"
      columns.each do |column|
        s << "    t.column #{activerecord_schema_definition(column)}"
      end
      s << "  end\nend"
      s
    end

    def sequel_schema(table_only = false) # :nodoc:
      s = ''
      s << "Sequel.migration do\n" unless table_only
      s << "  change do\n " unless table_only
      s << "    create_table(:#{name}) do\n"
      columns.each do |column|
        s << "      column #{sequel_schema_definition(column)}"
      end
      s << "    end\n"
      s << "  end\n" unless table_only
      s << "end\n" unless table_only
      s
    end

    def snowflake_schema(table_only = false) # :nodoc:
      s = ''
      s << "create or replace table enterprises (\n" unless table_only
      columns.each do |column|
        s << "  #{snowflake_schema_definition(column)}"
      end
      s << ")\n" unless table_only
      s
    end

    def json_schema(_table_only = false) # :nodoc:
      columns.map(&:to_hash).to_json
    end

    # ActiveRecord schema definition
    #
    # @param column [DBF::Column]
    # @return [String]
    def activerecord_schema_definition(column)
      "\"#{column.underscored_name}\", #{schema_data_type(column, :activerecord)}\n"
    end

    # Sequel schema definition
    #
    # @param column [DBF::Column]
    # @return [String]
    def sequel_schema_definition(column)
      ":#{column.underscored_name}, #{schema_data_type(column, :sequel)}\n"
    end

    # Snowflake schema definition
    #
    # @param column [DBF::Column]
    # @return [String]
    def snowflake_schema_definition(column)
      "#{column.underscored_name} #{schema_data_type(column, :snowflake)},\n"
    end

    def schema_data_type(column, format = :activerecord) # :nodoc:
      case column.type
      when *%w[N F I]
        number_data_type(format, column)
      when *%w[Y D T L M B]
        OTHER_DATA_TYPES[column.type]
      else
        string_data_format(format, column)
      end
    end

    def number_data_type(format, column)
      if format == :snowflake
        column.decimal > 0 ? 'float' : 'integer'
      else
        column.decimal > 0 ? ':float' : ':integer'
      end
    end

    def string_data_format(format, column)
      if format == :sequel
        ":varchar, :size => #{column.length}"
      elsif format == :snowflake
        "varchar(#{column.length})"
      else
        ":string, :limit => #{column.length}"
      end
    end
  end
end
