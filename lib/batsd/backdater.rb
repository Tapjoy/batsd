# USAGE:
# 
#   $: << '/path/to/batsd/lib'
#   
#   require 'batsd'
#   b = Batsd::Backdater.new(:root => '/path/to/batsd_data')
#   b.count('app_id.1.revenue', 100, Time.now - 7200)
# 
# *Note* that you need to run this on the BatsD server itself
# *Also Note* that you MUST use the correct retention policy as this tool
# writes directly to the disk where batsd is expecting to find data. If you 
# do not set the policy correctly, you will see the keys but be unable to find
# any data. The out-of-the-box retention policy should be:
#     retentions: {10 => 360, 60 => 10080, 600 => 52594},
# 
# QA example command:
# 
#   BUNDLE_GEMFILE=/opt/apps/batsd/current/Gemfile bundle exec irb
# 
# QA example script:
# 
#   $: << '/opt/apps/sensu_watcher/current/lib'
#   
#   require 'batsd'
#   b = Batsd::Backdater.new(:root => '/mnt/batsd')
#   b.count('test.app_id.1.revenue', 100, Time.now - 7200)
module Batsd
  class Backdater
    # Creates a new client to the given redis
    def initialize(options = {})
      options = {
        redis: {host: '127.0.0.1', port: 6379},
        retentions: {60 => 60, 300 => 864},
        root: 'tmp/statsd'
      }.merge(options)

      @retentions = options[:retentions]
      @redis = Batsd::Redis.new(
        :redis => options[:redis],
        # Make it appears as though there's a single retention since we immediately
        # write to disk
        :retentions => Hash[options[:retentions].to_a[0..0]]
      )
      @diskstore = Batsd::Diskstore.new(options[:root])
    end

    # Increments the given key by 1
    def increment(key, timestamp = Time.now)
      count(key, 1, timestamp)
    end

    # Decrements the given key by 1
    def decrement(key, timestamp = Time.now)
      count(key, -1, timestamp)
    end

    # Counts a key with the given value.  The default timestamp is the current
    # time.
    def count(key, value, timestamp = Time.now)
      key = "counters:#{key}"
      timestamp = timestamp.to_i
      value = value.to_i

      # Add datapoint
      @redis.add_datapoint(key)

      # Add value to redis store
      @redis.store_and_update_all_counters(timestamp, key, value) if store_in_redis?(timestamp)

      # Add value to filesystem store
      @retentions.each_with_index do |(retention, count), index|
        next if index.zero?
        append_value_to_file("#{key}:#{retention}", value, timestamp)
      end
      true
    end

    # Sends an arbitary gauge value for the given stat to the statsd server.
    def gauge(key, value, timestamp = Time.now)
      key = "gauges:#{key}"
      value = value.to_f
      timestamp = timestamp.to_i

      # Add value to filesystem store (gauges never get written to redis)
      append_value_to_file(key, value, timestamp)

      true
    end

    # Sends a timing (in ms) for the given stat to the statsd server.
    # +aggregation+ can be one of: :mean, :count, :min, :max, :upper_90, :stddev
    def timing(key, value, aggregation, timestamp = Time.now)
      key = "timers:#{key}"
      value = value.to_f
      timestamp = timestamp.to_i

      # Add value to redis store
      @redis.store_timer(timestamp, "#{key}:#{aggregation}", value) if store_in_redis?(timestamp)

      # Add value to filesystem store
      @retentions.each_with_index do |(retention, count), index|
        next if index.zero?
        append_value_to_file("#{key}:#{aggregation}:#{retention}", value, timestamp)
      end
      true
    end

    private
    # Should keys with the given timestamp be stored in redis?  This is based
    # on what the configured retention periods are
    def store_in_redis?(timestamp)
      timestamp > (Time.now - redis_period).to_i
    end

    def append_value_to_file(key, value, timestamp)
      timestamp = timestamp.to_i
      @diskstore.append_value_to_file(@diskstore.build_filename(key), "#{timestamp} #{value}")
    end

    # Gets how long of a period data gets stored in redis before it gets written
    # to disk
    def redis_period
      first_retention = @retentions.keys.sort.first
      first_retention * @retentions[first_retention]
    end
  end
end