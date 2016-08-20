module ActiveJob
  module TransactionsBehaviour
    extend ActiveSupport::Concern

    included do
      mattr_accessor(:transactions_behaviour) { :defer }
      thread_mattr_accessor(:pending_jobs, instance_accessor: false)
      self.pending_jobs = {}
    end

    def enqueue(options={})
      if should_defer_enqueuing?(options)
        defer_enqueuing(options)
      elsif should_raise?
        raise "You cannot enqueue the job while ActiveRecord transactions are opened"
      else
        ActiveJob::Base.pending_jobs.delete job_id
        super
      end
    end

    def defer_transaction_behaviour?
      self.class.transactions_behaviour == :defer
    end

    def enqueue_transaction_behaviour?
      self.class.transactions_behaviour == :enqueue
    end

    def raise_transaction_behaviour?
      self.class.transactions_behaviour == :raise
    end

    def compatible_transaction_behaviour?
      self.class.transactions_behaviour == :compatible
    end

    def should_defer_enqueuing?(options)
      !options[:force] && (defer_transaction_behaviour? || compatible_transaction_behaviour?) \
        && active_record_has_open_tranactions?
    end

    def active_record_has_open_tranactions?
      active_record_connections.any? do |pool|
        pool.connection.open_transactions > 0
      end
    end

    def provider_job_id
      if @job_deferred
        enqueue(@job_options.merge(force: true))
        ActiveJob::Base.pending_jobs.delete(job_id)
      end
      super
    end

    def defer_enqueuing(options)
      ActiveJob::Base.pending_jobs[job_id] = self
      @job_options = options
      @job_deferred = true
      ActiveSupport::Notifications.instrument "defer.active_job", adapter: self.class.queue_adapter, job: self
      @active_record_connections = active_record_connections_with_transactions
      active_record_connections_with_transactions.each do |connection|
        connection.current_transaction.on_commit do |transaction|
          if ActiveJob::Base.pending_jobs.key?(job_id)
            @active_record_connections.delete(transaction.connection)
            enqueue(options) if @active_record_connections.empty?
          end
        end
        connection.current_transaction.on_rollback do
          if ActiveJob::Base.pending_jobs.key?(job_id)
            ActiveSupport::Notifications.instrument "drop.active_job", adapter: self.class.queue_adapter, job: self
            ActiveJob::Base.pending_jobs.delete job_id
          end
        end
      end
    end

    def active_record_connections
      ActiveRecord::Base.connection_handler.connection_pool_list.map(&:connection)
    end

    def active_record_connections_with_transactions
      active_record_connections.select { |c| c.open_transactions > 0 }
    end
  end
end
