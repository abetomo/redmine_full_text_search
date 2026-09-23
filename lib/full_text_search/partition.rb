module FullTextSearch
  class Partition
    MIGRATION_VERSION = 20260909151100

    # To use "logical_select", the following features are required.
    # * `slices`
    # * the dynamic columns of the `output` stage
    # * `query_flags`
    # These features were added in the following version.
    GROONGA_REQUIRED_VERSION = "16.1.1"

    # "pgroonga_physical_table_names" is added in this version.
    # It's needed to specify the Groonga table of each partition.
    PGROONGA_REQUIRED_VERSION = "4.0.6"

    class << self
      def available?
        return false unless Redmine::Database.postgresql?
        return false unless physical_table_names_available?
        return false unless ensure_sharding_plugin_registered
        logical_select_features_are_supported?
      end

      def table_name
        Target.table_name.to_s
      end

      def partitioned?
        return false unless Redmine::Database.postgresql?
        if @partition.nil?
          @partition = (connection.select_value(<<~SQL) == "p")
SELECT relkind
  FROM pg_class
 WHERE oid = to_regclass(#{connection.quote(table_name)});
          SQL
        end
        @partition
      end

      def ensure_sharding_plugin_registered
        return true if @sharding_plugin_registered

        response = connection.select_value(<<~SQL)
SELECT pgroonga_command('plugin_register', ARRAY['name', 'sharding']);
        SQL
        header, body = JSON.parse(response)
        @sharding_plugin_registered = (header[0].zero? && body == true)
      rescue ActiveRecord::StatementInvalid, JSON::ParserError, TypeError
        false
      end

      def logical_select_features_are_supported?
        if @logical_select_features_are_supported.nil?
          @logical_select_features_are_supported =
            (Gem::Version.new(Target.groonga_version) >=
             Gem::Version.new(GROONGA_REQUIRED_VERSION))
        end
        @logical_select_features_are_supported
      end

      def ensure_created(year)
        ensure_created_internal(year)
      rescue ActiveRecord::StatementInvalid => error
        Rails.logger.warn("[full-text-search][partition][create] " +
                          "failed to create #{partition_name(year)}: " +
                          "#{error}")
        false
      end

      def requirements_message
        "partitioning requires " +
          "Groonga #{GROONGA_REQUIRED_VERSION} or later and " +
          "PGroonga #{PGROONGA_REQUIRED_VERSION} or later"
      end

      private
      def physical_table_names_available?
        connection.select_value(<<~SQL).present?
SELECT to_regprocedure('pgroonga_physical_table_names(text, text)');
        SQL
      end

      def partition_name(year)
        "#{table_name}_#{year}"
      end

      def default_partition_name
        "#{table_name}_default"
      end

      def ensured_years
        @ensured_years ||= Concurrent::Set.new
      end

      def ensure_created_internal(year)
        return false unless partitioned?
        return :exist unless ensured_years.add?(year)
        return :exist if connection.data_source_exists?(partition_name(year))
        create(year)
        :created
      end

      def create(year)
        name = partition_name(year)
        connection.transaction(requires_new: true) do
          connection.execute(<<~SQL)
CREATE TABLE #{name} (
  LIKE #{table_name}
    INCLUDING DEFAULTS
    INCLUDING CONSTRAINTS
);
          SQL

          # Move the records because having them in the default table causes an error.
          move_default_records(year)

          connection.execute(<<~SQL)
ALTER TABLE #{table_name}
ATTACH PARTITION #{name}
FOR VALUES FROM ('#{year}-01-01') TO ('#{year + 1}-01-01');
          SQL
        end
      end

      def move_default_records(year)
        condition = "registered_at >= '#{year}-01-01' AND " +
                    "registered_at < '#{year + 1}-01-01'"
        connection.execute(<<~SQL)
INSERT INTO #{partition_name(year)}
SELECT *
  FROM #{default_partition_name}
 WHERE #{condition};
        SQL
        connection.execute(<<~SQL)
DELETE FROM #{default_partition_name}
 WHERE #{condition};
        SQL
      end

      def connection
        ActiveRecord::Base.connection
      end
    end
  end
end
