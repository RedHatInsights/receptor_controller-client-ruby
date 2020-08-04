require 'base64'
require "concurrent"
require 'stringio'
require 'zlib'

module ReceptorController
  # ResponseWorker is listening on Kafka topic platform.receptor-controller.responses (@see Configuration.queue_topic)
  # It asynchronously receives responses requested by POST /job to receptor controller.
  # Request and response is paired by message ID (response of POST /job and 'in_response_to' value in kafka response here)
  #
  # Successful responses are at least two:
  # * 1+ of 'response' type, containing data
  # * 1 of 'eof' type, signalizing end of transmission
  #
  # Registered messages without response are removed after timeout (Configuration.response_timeout)
  #
  # All type of responses/timeout can be sent to registered callbacks (@see :register_message)
  #
  # Use "start" and "stop" methods to start/stop listening on Kafka
  class Client::ResponseWorker
    EOF = "eof".freeze
    RESPONSE = "response".freeze

    attr_reader :started
    alias started? started

    attr_accessor :received_messages

    def initialize(config, logger)
      self.config              = config
      self.lock                = Mutex.new
      self.timeout_lock        = Mutex.new
      self.logger              = logger
      self.registered_messages = Concurrent::Map.new
      self.received_messages   = Concurrent::Array.new
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
    # @param error_callback [Symbol] name of receiver's method processing errors [optional]
    def register_message(msg_id, receiver, response_callback: :response_success, timeout_callback: :response_timeout, error_callback: :response_error)
      registered_messages[msg_id] = {:receiver          => receiver,
                                     :response_callback => response_callback,
                                     :timeout_callback  => timeout_callback,
                                     :error_callback    => error_callback,
                                     :last_checked_at   => Time.now.utc}
    end

    private

    attr_accessor :config, :lock, :logger, :registered_messages, :timeout_lock, :workers
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

      if (message_id = response['in_response_to'])
        # logger.debug("Receptor response for message #{message_id}: serial: #{response["serial"]}, type: #{response['message_type']}, payload: #{response['payload'] || "n/a"}")
        if (callbacks = registered_messages[message_id]).present?
          # Reset last_checked_at to avoid timeout in multi-response messages
          reset_last_checked_at(callbacks)

          if response['code'] == 0
            #
            # Response OK
            #
            message_type = response['message_type'] # "response" (with data) or "eof" (without data)
            payload = response['payload']
            serial = response['serial']
            callbacks[:received_msgs] ? callbacks[:received_msgs] += 1 : callbacks[:received_msgs] = 1

            case message_type
            when EOF
              # Store how many messages are needed to be received for this request
              callbacks[:total_msgs] = serial
            when RESPONSE
              payload = unpack_payload(payload) if payload.kind_of?(String)
              callbacks[:msg_size] ? callbacks[:msg_size] += payload["body"].size : callbacks[:msg_size] = payload["body"].size
              callbacks[:receiver].send(callbacks[:response_callback], message_id, message_type, serial, payload)
            end

            # We received all the messages, complete the message.
            if callbacks[:received_msgs] == callbacks[:total_msgs]
              registered_messages.delete(message_id)
              callbacks[:receiver].send(callbacks[:response_callback], message_id, EOF, serial, payload)
              logger.debug("Receptor Message #{message_id} complete, total bytes: #{callbacks[:msg_size]}")
            end

            logger.debug("Receptor response: OK | message: #{message_id}, serial: #{response["serial"]}, type: #{message_type}, payload: #{payload || "n/a"}")
          else
            #
            # Response Error
            #
            registered_messages.delete(message_id)

            logger.error("Receptor response: ERROR | message #{message_id} (#{response})")

            callbacks[:receiver].send(callbacks[:error_callback], message_id, response['code'], response['serial'], response['payload'])
          end
        elsif ENV["LOG_ALL_RECEPTOR_MESSAGES"]&.to_i != 0
          # noop, it's not error if not registered, can be processed by another pod
          logger.debug("Receptor response unhandled: #{message_id} (#{response['code']})")
        end
      else
        logger.error("Receptor response: Message id (in_response_to) not received! #{response}")
      end
    rescue JSON::ParserError => e
      logger.error("Receptor response: Failed to parse Kafka response (#{e.message})\n#{message.payload}")
    rescue => e
      logger.error("Receptor response: #{e}\n#{e.backtrace.join("\n")}")
    ensure
      message.ack unless config.queue_auto_ack
    end

    def check_timeouts(threshold = config.response_timeout)
      expired = []
      #
      # STEP 1 Collect expired messages
      #
      registered_messages.each_pair do |message_id, callbacks|
        timeout_lock.synchronize do
          if callbacks[:last_checked_at] < Time.now.utc - threshold
            expired << message_id
          end
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
    rescue => err
      logger.error("Exception in maintenance worker: #{err}\n#{err.backtrace.join("\n")}")
    end

    # GZIP recognition
    # https://tools.ietf.org/html/rfc1952#page-5
    def gzipped?(data)
      sign = data.to_s.bytes[0..1]

      sign[0] == '0x1f'.hex && sign[1] == '0x8b'.hex
    end

    # Tries to decompress String response
    # If not a gzip, it's a String error from receptor node
    def unpack_payload(data)
      decoded = Base64.decode64(data)
      if gzipped?(decoded)
        gz = Zlib::GzipReader.new(StringIO.new(decoded))
        JSON.parse(gz.read)
      else
        data
      end
    end

    # Reset last_checked_at to avoid timeout in multi-response messages
    def reset_last_checked_at(callbacks)
      timeout_lock.synchronize do
        callbacks[:last_checked_at] = Time.now.utc
      end
    end

    # No persist_ref here, because all instances (pods) needs to receive kafka message
    def queue_opts
      opts               = {:service  => config.queue_topic,
                            :auto_ack => config.queue_auto_ack}
      opts[:max_bytes]   = config.queue_max_bytes if config.queue_max_bytes
      opts[:persist_ref] = config.queue_persist_ref if config.queue_persist_ref
      opts
    end

    def default_messaging_opts
      {
        :host       => config.queue_host,
        :port       => config.queue_port,
        :protocol   => :Kafka,
        :client_ref => "receptor_client-responses-#{Time.now.to_i}", # A reference string to identify the client
      }
    end
  end
end
