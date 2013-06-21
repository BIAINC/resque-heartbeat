require 'resque'

module Resque
  class Worker
    def startup_with_heartbeat
      startup_without_heartbeat
      heart.run
    end
    alias_method(:startup_without_heartbeat, :startup)
    alias_method(:startup, :startup_with_heartbeat)

    def unregister_worker_with_heartbeat
      heart.stop
      unregister_worker_without_heartbeat
    end
    alias_method(:unregister_worker_without_heartbeat, :unregister_worker)
    alias_method(:unregister_worker, :unregister_worker_with_heartbeat)

    def heart
      @heart ||= Heart.new(self)
    end

    def prune_if_dead
      return nil unless heart.last_beat_before?(5)

      Resque.logger.info "Pruning worker '#{hostname}' from resque. Last heartbeat was at #{heart.last_beat}"
      unregister_worker
    end

    class Heart
      attr_reader :worker

      def initialize(worker)
        @worker = worker
      end

      def run
        @thrd ||= Thread.new do
          loop do
            begin
              sleep(2) && beat!
            rescue Exception => e
              Resque.logger.error "Error while doing heartbeat: #{e} : #{e.backtrace}"
            end
          end
        end
      end

      def stop
        Thread.kill(@thrd)
        puts @thrd
        Resque.redis.del key
      rescue
        nil
      end

      def redis
        Resque.redis
      end

      def Heart.heartbeat_key(worker_name)
        "worker:#{worker_name}:heartbeat"
      end

      def key
        Heart.heartbeat_key worker
      end

      def beat!
        redis.sadd(:workers, worker)
        redis.set(key, Time.now.to_s)
      rescue Exception => e
        Resque.logger.fatal "Unable to set the heartbeat for worker '#{hostname}': #{e} : #{e.backtrace}"
      end

      def last_beat_before?(seconds)
        Time.parse(last_beat).utc < (Time.now.utc - seconds) rescue true
      end

      def last_beat
        Resque.redis.get(key) || worker.started
      end
    end
  end

  # NOTE: this assumes all of your workers are putting out heartbeats
  def self.prune_dead_workers
    begin
      beats = Resque.redis.keys(Worker::Heart.heartbeat_key('*'))
      Worker.all.each do |worker|
        worker.prune_if_dead

        # remove the worker from consideration
        beats.delete worker.heart.key
      end

      # remove `
      beats.each do |key|
        Resque.logger.info "Removing #{key} from heartbeats because the worker isn't talking to Resque."
        Resque.redis.del key
      end
    rescue Exception => e
      p e
    end
  end

end
