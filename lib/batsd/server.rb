require 'json'
module Batsd
  # Makes data from statsd available over a TCP socket
  module Server
    # access the server config
    def self.config
      @config
    end

    # Set the config for the server
    def self.config=(config)
      @config=config
    end
   
    # Set up a redis and diskstore instance per connection
    # so they don't step on each other. Since redis commands
    # are happening in a deferrable, intentionally not using EM-redis
    def post_init
      puts "batsd server ready and waiting on #{Batsd::Server.config[:port]} to ship data upon request\n"
      @redis = Batsd::Redis.new(Batsd::Server.config)
      @diskstore = Batsd::Diskstore.new(Batsd::Server.config[:root])
    end
    
    # Handle a command received over the server port and return
    # the datapoints, values, or a PONG as requested.
    def receive_data(msg)  
      msg.split("\n").each do |row|
        begin
          msg_split = row.split(" ")
          command = msg_split[0]

          return unless command
          case
            when command.match(/available/i)
              EM.defer { send_data "#{JSON(@redis.datapoints)}\n" }
            when command.match(/values/i)
              EM.defer do
                 command, metric, begin_time, end_time = msg_split
                 datapoints, interval = [], 0

                 if metric.match(/^gauge/)
                   datapoints = @diskstore.read(metric, begin_time, end_time)
                 else
                   Batsd::Server.config[:retentions].each_with_index do |retention, index|
                     if (index != Batsd::Server.config[:retentions].count - 1) && (Time.now.to_i - (retention[0] * retention[1]) > begin_time.to_i)
                       next
                     end
                     interval = retention[0]

                     if index.zero?
                       datapoints = @redis.values_from_zset(metric, begin_time, end_time)
                       break
                     else
                       datapoints = @diskstore.read("#{metric}:#{retention[0]}", begin_time, end_time)
                       break
                     end
                   end
                 end
                 send_data "#{JSON({'interval' => interval, "#{metric}" => datapoints})}\n"
              end
            when command.match(/ping/i)
              send_data "PONG\n"
            when command.match(/delete/i)
              EM.defer do
                command, metric = msg_split
                delete_metric(metric)
              end
            when command.match(/quit|exit/i)
              send_data "BYE\n"
              close_connection
            else
              send_data "#{JSON({error: "Unrecognized command #{command}"})}\n"
          end
        rescue Exception => e
          puts e if ENV["VERBOSE"]
        rescue
          puts "Uncaught Error"
        end
      end
    end

    # Remove all data and references to the given metric at all retention intervals.
    # 
    # Note that this will work but if the receiver process is still running and has
    # information about this metric, it could repopulate them at some future flush interval.
    # To be sure turn off the receiver process when deleting metrics.
    def delete_metric(metric)
      puts "Deleting metric #{metric}" if ENV["VERBOSE"]
      if metric.match(/^gauge/)
        @diskstore.delete(metric)
      else
        Batsd::Server.config[:retentions].each do |retention|
          @diskstore.delete("#{metric}:#{retention[0]}")
        end
      end
      @redis.remove_datapoint(metric)
    end


    # Bind to port+2 and serve up data over TCP. Offers access to
    # both the set of datapoints and the values as JSON arrays.
    class Daemon

      # Create a new daemon and expose options
      def initialize(options={})
        @options = options
        @bind = @options[:bind] || '0.0.0.0'
        @port = @options[:manual_port] || ((@options[:port] || 8125) + 2)
        Batsd::Server.config = @options.merge(port: @port)
      end

      # Run the server
      def run
        puts "Starting server on #{@port}"
        EventMachine.threadpool_size = 100
        EventMachine::run do
          EventMachine::start_server(@bind, @port, Batsd::Server)  
        end
      end
    end
  end 
end

