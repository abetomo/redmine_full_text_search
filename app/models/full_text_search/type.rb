module FullTextSearch
  class Type < ApplicationRecord
    self.table_name = :fts_types

    class << self
      private def normalize_key(key)
        case key
        when Class
          key.name.underscore
        when ActiveRecord::Base
          key.class.name.underscore
        when /\A[A-Z]/
          key.underscore
        else
          key.singularize
        end
      end

      def [](key)
        __send__(normalize_key(key))
      end

      def available?(name)
        respond_to?(normalize_key(name))
      end

      def attachment
        find_or_create_by(name: "Attachment")
      end

      def change
        find_or_create_by(name: "Change")
      end

      def changeset
        find_or_create_by(name: "Changeset")
      end

      def custom_value
        find_or_create_by(name: "CustomValue")
      end

      def document
        find_or_create_by(name: "Document")
      end

      def file
        find_or_create_by(name: "File")
      end

      def issue
        find_or_create_by(name: "Issue")
      end

      def journal
        find_or_create_by(name: "Journal")
      end

      def message
        find_or_create_by(name: "Message")
      end

      def news
        find_or_create_by(name: "News")
      end

      def project
        find_or_create_by(name: "Project")
      end

      def repository
        find_or_create_by(name: "Repository")
      end

      def version
        find_or_create_by(name: "Version")
      end

      def wiki_page
        find_or_create_by(name: "WikiPage")
      end
    end
  end
end
