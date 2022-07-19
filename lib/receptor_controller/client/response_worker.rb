require 'active_support/notifications'
require 'base64'
require "concurrent"
require 'stringio'
require 'zlib'
require 'rdkafka'
require 'manageiq/messaging/kafka/common'
require 'clowder-common-ruby'

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
    include ManageIQ::Messaging::Kafka::Common
    RECEPTOR_RESPONSES_TOPIC = "platform.receptor-controller.responses".freeze

    EOF = "eof".freeze
    RESPONSE = "response".freeze
    INITIALIZATION_TIMEOUT = 20

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
      init_lock, init_wait = Mutex.new, ConditionVariable.new

      lock.synchronize do
        return if started.value

        started.value         = true
        workers[:maintenance] = Thread.new { check_timeouts while started.value }
        workers[:listener]    = Thread.new { listen while started.value }
      end

      logger.info("Receptor Response worker started...")
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
      client = Rdkafka::Config.new(queue_opts).consumer
      topic = kafka_topic(RECEPTOR_RESPONSES_TOPIC)

      client.subscribe(topic)
      client.each do |message|
        self.send(:process_topic_message, client, topic, message) do |parsed|
          process_message(parsed)
        end
      end
    rescue => err
      logger.error(response_log("Exception in kafka listener: #{err}\n#{err.backtrace.join("\n")}"))
    ensure
      client&.close
    end

    def process_message(message)
      response = JSON.parse(message.payload)

      if (message_id = response['in_response_to'])
        callbacks = registered_messages[message_id]
        log_received_message(callbacks, message_id, response)

        if callbacks.present?
          # Reset last_checked_at to avoid timeout in multi-response messages
          reset_last_checked_at(callbacks)

          if response['code'] == 0
            #
            # Response OK
            #
            message_type = response['message_type'] # "response" (with data) or "eof" (without data)
            payload = response['payload']
            callbacks[:received_msgs] ? callbacks[:received_msgs] += 1 : callbacks[:received_msgs] = 1

            case message_type
            when EOF
              # Store how many messages are needed to be received for this request
              callbacks[:total_msgs] = response["serial"]
            when RESPONSE
              payload = unpack_payload(payload) if payload.kind_of?(String)
              callbacks[:msg_size] ? callbacks[:msg_size] += payload.size : callbacks[:msg_size] = payload.size
              callbacks[:receiver].send(callbacks[:response_callback], message_id, message_type, payload)
            else
              # Send the callback to release the thread.
              logger.warn(response_log("Unexpected type | message #{message_id}, type: #{message_type}"))
              callbacks[:receiver].send(callbacks[:response_callback], message_id, message_type, payload)
            end

            # We received all the messages, complete the message.
            if callbacks[:received_msgs] == callbacks[:total_msgs]
              registered_messages.delete(message_id)
              callbacks[:receiver].send(callbacks[:response_callback], message_id, EOF, payload)
              logger.debug(response_log("Message #{message_id} complete, total bytes: #{callbacks[:msg_size]}"))
            end

            logger.debug(response_log("OK | message: #{message_id}, serial: #{response["serial"]}, type: #{message_type}, payload: #{payload || "n/a"}"))
          else
            #
            # Response Error
            #
            registered_messages.delete(message_id)

            logger.error(response_log("ERROR | message #{message_id} (#{response})"))

            callbacks[:receiver].send(callbacks[:error_callback], message_id, response['code'], response['payload'])
          end
        elsif (ENV["LOG_ALL_RECEPTOR_MESSAGES"] || 0)&.to_i != 0
          # noop, it's not error if not registered, can be processed by another pod
          logger.debug(response_log("NOT REGISTERED | #{message_id} (#{response['code']})"))
        end
      else
        logger.error(response_log("MISSING | Message id (in_response_to) not received! #{response}"))
      end
    rescue JSON::ParserError => e
      logger.error(response_log("Failed to parse Kafka response (#{e.message})\n#{message.payload}"))
    rescue => e
      logger.error(response_log("#{e}\n#{e.backtrace.join("\n")}"))
    end

    def check_timeouts(threshold = 2.minutes)
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

      sleep(10)
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

    def queue_opts
       # parsing the clowder config file and building the rdkafka args, docs here: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

      # the default params, overriding what we need to in the tap block...
      {
        :"group.id" => ENV['HOSTNAME'].presence || SecureRandom.hex(4),
        :"client.id" => ENV['HOSTNAME'].presence || SecureRandom.hex(4),
        :"enable.auto.commit" => true
      }.tap do |params|
        if ClowderCommonRuby::Config.clowder_enabled?
          broker = config.kafka.brokers[0]
          params[:"bootstrap.servers"] = "#{broker.hostname}:#{broker.port}"

          if broker.authtype == "sasl" && broker.respond_to?(:sasl) && broker.sasl.present?
            params[:"sasl.username"] = broker.sasl.username
            params[:"sasl.password"] = broker.sasl.password
            params[:"sasl.mechanism"] = broker.sasl.saslMechanism
            params[:"security.protocol"] = broker.sasl.securityProtocol
            params[:"ssl.certificate.pem"] = broker.cacert if broker.respond_to?(:cacert) && broker.cacert.present?
          end
        else
          params[:"bootstrap.servers"] = "#{ENV["QUEUE_HOST"]}:#{ENV["QUEUE_PORT"]}"
        end
      end
    end

    def default_messaging_opts
      return @default_messaging_opts if @default_messaging_opts

      @default_messaging_opts = {
        :host       => config.queue_host,
        :port       => config.queue_port,
        :protocol   => :Kafka,
        :client_ref => "receptor_client-responses-#{Time.now.to_i}", # A reference string to identify the client
      }
    end

    def log_received_message(callbacks, message_id, response)
      log_all = (ENV["LOG_ALL_RECEPTOR_MESSAGES"] || 0).to_i != 0
      if log_all || (!log_all && callbacks.present?)
        logger.debug(response_log("Received message #{message_id}: serial: #{response["serial"]}, type: #{response['message_type']}, payload: #{response['payload'] || "n/a"}"))
      end
    end

    def response_log(message)
      "Receptor Response [#{queue_opts[:persist_ref]}]: #{message}"
    end

    def config
      @config ||= ClowderCommonRuby::Config.load if ClowderCommonRuby::Config.clowder_enabled?
    end

    def kafka_topic(name)
      if ClowderCommonRuby::Config.clowder_enabled?
        config.kafka.topics.find {|t| t.requestedName == name}&.name || name
      else
        name
      end
    end
  end
end
