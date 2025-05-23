module FullTextSearch
  module IssueSynchronizable
    extend ActiveSupport::Concern

    included do
      after_commit :queue_sync_on_commit
      after_destroy :queue_sync_on_destroy
    end

    private

    def queue_sync(action)
      FullTextSearch::UpdateIssueContentJob.perform_later(
        self.class.name,
        id,
        action
      )
    end

    def queue_sync_on_commit
      queue_sync("commit")
    end

    def queue_sync_on_destroy
      queue_sync("destroy")
    end
  end
end
