require "receptor_controller/client/directive"

module ReceptorController
  # Blocking directive for requests through POST /job
  # Successful POST causes locking current thread until response from Kafka comes
  #
  # Raises kind of ReceptorController::Client::Error in case of problems/timeout
  class Client::DirectiveBlocking < Client::Directive
    def initialize(name:, account:, node_id:, payload:, client:, log_message_common: nil)
      super
      self.response_lock = Mutex.new
      self.response_waiting = ConditionVariable.new
      self.response_data = nil
      self.response_exception = nil
    end

    def call(body = default_body)
      @url = JSON.parse(body[:payload])['url']
      response = connection.post(config.job_path, body.to_json)

      msg_id = JSON.parse(response.body)['id']
      href_slug = JSON.parse(body[:payload])['href_slug']

      logger.debug("Receptor response [#{ReceptorController::Client::Configuration.default.queue_persist_ref}]: registering message #{msg_id}, href_slug: #{href_slug}")
      # registers message id for kafka responses
      response_worker.register_message(msg_id, self)
      wait_for_response(msg_id)
    rescue Faraday::Error => e
      msg = receptor_log_msg("Directive #{name} failed (#{log_message_common}) [MSG: #{msg_id}]", account, node_id, e)
      raise ReceptorController::Client::ControllerResponseError.new(msg)
    end

    def wait_for_response(_msg_id)
      response_lock.synchronize do
        response_waiting.wait(response_lock)

        raise response_exception if response_failed?

        response_data.dup
      end
    end

    # TODO: Review when future plugins with more "response" messages come
    def response_success(msg_id, message_type, response)
      response_lock.synchronize do
        if message_type == MESSAGE_TYPE_RESPONSE
          self.response_data = response
        elsif message_type == MESSAGE_TYPE_EOF
          response_waiting.signal
        else
          self.response_exception = ReceptorController::Client::UnknownResponseTypeError.new("#{log_message_common}[MSG: #{msg_id}]")
          response_waiting.signal
        end
      end
    end

    def response_error(msg_id, response_code, err_message)
      response_lock.synchronize do
        self.response_data = nil
        self.response_exception = ReceptorController::Client::ResponseError.new("#{err_message} (code: #{response_code}) (#{log_message_common}) [MSG: #{msg_id}]")
        response_waiting.signal
      end
    end

    def response_timeout(msg_id)
      response_lock.synchronize do
        self.response_data = nil
        self.response_exception = ReceptorController::Client::ResponseTimeoutError.new("Timeout (#{log_message_common}) [MSG: #{msg_id}]")
        response_waiting.signal
      end
    end

    private

    attr_accessor :response_data, :response_exception, :response_lock, :response_waiting

    def connection
      @connection ||= Faraday.new(config.controller_url, :headers => client.headers) do |c|
        c.use(Faraday::Response::RaiseError)
        c.adapter(Faraday.default_adapter)
      end
    end

    def response_failed?
      response_exception.present?
    end
  end
end
