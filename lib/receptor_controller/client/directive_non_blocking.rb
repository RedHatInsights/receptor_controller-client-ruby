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
    def initialize(name:, account:, node_id:, payload:, client:)
      super

      @success_callbacks = []
      @eof_callbacks = []
      @timeout_callbacks = []
      @error_callbacks = []
    end

    def call(body = default_body)
      response = Faraday.post(config.job_url, body.to_json, client.headers)
      if response.success?
        msg_id = JSON.parse(response.body)['id']

        # registers message id for kafka responses
        response_worker.register_message(msg_id, self)

        msg_id
      else
        logger.error(receptor_log_msg("Directive #{name} failed: HTTP #{response.status}", account, node_id))
        nil
      end
    rescue Faraday::Error => e
      logger.error(receptor_log_msg("Directive #{name} failed. POST /job error", account, node_id, e))
      nil
    rescue => e
      logger.error(receptor_log_msg("Directive #{name} failed", account, node_id, e))
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

    def response_success(msg_id, message_type, response)
      if message_type == MESSAGE_TYPE_EOF
        @eof_callbacks.each { |block| block.call(msg_id) }
      else
        @success_callbacks.each { |block| block.call(msg_id, response) }
      end
    end

    def response_error(msg_id, response_code, response)
      @error_callbacks.each { |block| block.call(msg_id, response_code, response) }
    end

    def response_timeout(msg_id)
      @timeout_callbacks.each { |block| block.call(msg_id) }
    end
  end
end
