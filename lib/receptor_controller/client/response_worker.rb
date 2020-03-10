require "concurrent"

module ReceptorController
  class Client::ResponseWorker
    attr_reader :started
    alias_method :started?, :started

    def initialize(config, logger)
      self.config              = config
      self.lock                = Mutex.new
      self.logger              = logger
      self.registered_messages = Concurrent::Map.new
      self.started             = Concurrent::AtomicBoolean.new(false)
      self.workers             = {}
    end

    # Start listening on Kafka
    def start
      lock.synchronize do
        return if started.value

        started.value         = true
        workers[:maintenance] = Thread.new { check_timeouts while started.value }
        workers[:listener]    = Thread.new { listen while started.value }
      end
    end

    # Stop listener
    def stop
      lock.synchronize do
        return unless started.value

        started.value = false
        workers[:listener]&.terminate
        workers[:maintenance]&.join
      end
    end

    # Registers message_id received by request,
    # Defines response and timeout callback methods
    #
    # @param msg_id [String] UUID
    # @param receiver [Object] any object implementing callbacks
    # @param response_callback [Symbol] name of receiver's method processing responses
    # @param timeout_callback [Symbol] name of receiver's method processing timeout [optional]
    def register_message(msg_id, receiver, response_callback: :response_received, timeout_callback: :response_timeout)
      logger.debug("Receptor response: registering message #{msg_id}")
      registered_messages[msg_id] = {:receiver => receiver, :response_callback => response_callback, :timeout_callback => timeout_callback, :registered_at => Time.now.utc}
    end

    private

    attr_accessor :config, :lock, :logger, :registered_messages, :workers
    attr_writer :started

    def listen
      # Open a connection to the messaging service
      client = ManageIQ::Messaging::Client.open(default_messaging_opts)

      logger.info("Receptor Response worker started...")
      client.subscribe_topic(queue_opts) do |message|
        process_message(message)
      end
    rescue => err
      logger.error("Exception in kafka listener: #{err}\n#{err.backtrace.join("\n")}")
    ensure
      client&.close
    end

    def process_message(message)
      response = JSON.parse(message.payload)
      if response['code'] == 0
        message_id = response['in_response_to']
        # message_type: "response" (with data) or
        #               "eof"(without data)
        message_type = response['message_type']

        if message_id
          if (callbacks = registered_messages[message_id]).present?
            logger.debug("Receptor response: processing message #{message_id} (#{response})")
            registered_messages.delete(message_id) if message_type == 'eof'
            # Callback to sender
            callbacks[:receiver].send(callbacks[:response_callback], message_id, message_type, response['payload'])
          end
        else
          raise "Message id (in_response_to) not received! #{response}"
        end
      else
        logger.error("Receptor_satellite:health_check directive failed in receptor_client node #{response['sender']}")
      end
    rescue JSON::ParserError => e
      logger.error("Failed to parse Kafka response (#{e.message})\n#{message.payload}")
    rescue => e
      logger.error("#{e}\n#{e.backtrace.join("\n")}")
    end

    def check_timeouts(threshold = config.response_timeout)
      while started.value
        expired = []
        #
        # STEP 1 Collect expired messages
        #
        registered_messages.each_pair do |message_id, callbacks|
          if callbacks[:registered_at] < Time.now.utc - threshold
            expired << message_id
          end
        end

        #
        # STEP 2 Remove expired messages, send timeout callbacks
        #
        expired.each do |message_id|
          callbacks = registered_messages.delete(message_id)
          if callbacks[:receiver].respond_to?(callbacks[:timeout_callback])
            callbacks[:receiver].send(callbacks[:timeout_callback], message_id)
          end
        end

        sleep(config.response_timeout_poll_time)
      end
    rescue => err
      logger.error("Exception in maintenance worker: #{err}\n#{err.backtrace.join("\n")}")
    end

    # No persist_ref here, because all instances (pods) needs to receive kafka message
    def queue_opts
      opts = {:service => config.queue_topic}
      opts[:max_bytes] = config.queue_max_bytes if config.queue_max_bytes
      opts[:persist_ref] = config.queue_persist_ref if config.queue_persist_ref
      opts
    end

    def default_messaging_opts
      {
        :host       => config.queue_host,
        :port       => config.queue_port,
        :protocol   => :Kafka,
        :client_ref => "tp-inventory-receptor_client-responses-#{Time.now.to_i}", # A reference string to identify the client
      }
    end
  end
end
