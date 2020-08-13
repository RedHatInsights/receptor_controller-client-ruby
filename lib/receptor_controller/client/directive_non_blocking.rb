require "concurrent"
require "receptor_controller/client/directive"

module ReceptorController
  # Non-blocking directive for requests through POST /job
  # Directive's call returns either message ID or nil
  #
  # Callback blocks can be specified for handling responses
  # @example:
  #         receiver = <object with methods below>
  #         directive
  #           .on_success do |msg_id, response|
  #             receiver.process_response(msg_id, response)
  #           end
  #           .on_error do |msg_id, code, response|
  #             receiver.process_error(msg_id, code, response)
  #           end
  #           .on_timeout do |msg_id|
  #             receiver.process_timeout(msg_id)
  #           end
  #           .on_eof do |msg_id|
  #             receiver.process_eof(msg_id)
  #           end
  #           .on_eof do |msg_id|
  #             logger.debug("[#{msg_id}] EOF message received")
  #           end
  #
  #         directive.call
  class Client::DirectiveNonBlocking < Client::Directive
    def initialize(name:, account:, node_id:, payload:, client:, log_message_common: nil)
      super

      @success_callbacks = []
      @eof_callbacks     = []
      @timeout_callbacks = []
      @error_callbacks   = []

      @responses_count = Concurrent::AtomicFixnum.new
      @eof_lock        = Mutex.new
      @eof_wait        = ConditionVariable.new
    end

    # Entrypoint for request
    def call(body = default_body)
      response = Faraday.post(config.job_url, body.to_json, client.headers)
      if response.success?
        msg_id = JSON.parse(response.body)['id']
        href_slug = JSON.parse(body[:payload])['href_slug']

        # registers message id for kafka responses
        response_worker.register_message(msg_id, self)
        logger.debug("Receptor response [#{ReceptorController::Client::Configuration.default.queue_persist_ref}]: registering message #{msg_id}, href_slug: #{href_slug}")

        msg_id
      else
        logger.error(receptor_log_msg("Directive #{name} failed (#{log_message_common}): HTTP #{response.status}", account, node_id))
        nil
      end
    rescue Faraday::Error => e
      logger.error(receptor_log_msg("Directive #{name} failed (#{log_message_common}). POST /job error", account, node_id, e))
      nil
    rescue => e
      logger.error(receptor_log_msg("Directive #{name} failed (#{log_message_common})", account, node_id, e))
      nil
    end

    def on_success(&block)
      @success_callbacks << block if block_given?
      self
    end

    def on_eof(&block)
      @eof_callbacks << block if block_given?
      self
    end

    def on_timeout(&block)
      @timeout_callbacks << block if block_given?
      self
    end

    def on_error(&block)
      @error_callbacks << block if block_given?
      self
    end

    # Handles successful responses in Threads
    # EOF processing waits until all response threads are finished
    def response_success(msg_id, message_type, response)
      if message_type == MESSAGE_TYPE_EOF
        eof_thread do
          @eof_callbacks.each { |block| block.call(msg_id) }
        end
      else
        response_thread do
          @success_callbacks.each { |block| block.call(msg_id, response) }
        end
      end
    end

    # Handles error responses in Threads
    # EOF processing waits until all threads are finished
    def response_error(msg_id, response_code, response)
      response_thread do
        @error_callbacks.each { |block| block.call(msg_id, response_code, response) }
      end
    end

    # Error state: Any response wasn't received in `Configuration.response_timeout`
    def response_timeout(msg_id)
      response_thread do
        @timeout_callbacks.each { |block| block.call(msg_id) }
      end
    end

    private

    # Responses are processed in threads to be able to call subrequests
    # EOF response is blocked by thread-safe counter
    def response_thread
      @responses_count.increment

      Thread.new do
        yield
      ensure
        @responses_count.decrement
        @eof_lock.synchronize do
          @eof_wait.signal if @responses_count.value == 0
        end
      end
    end

    # Messages in kafka are received serialized, EOF is always last
    # => @responses_count has to be always positive
    #    until all responses are processed
    def eof_thread
      Thread.new do
        @eof_lock.synchronize do
          @eof_wait.wait(@eof_lock) if @responses_count.value > 0
        end

        yield
      end
    end
  end
end
