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
      self.pages = {}
    end

    def call(body = default_body)
      @url = JSON.parse(body[:payload])['url']
      response = connection.post(config.job_path, body.to_json)

      msg_id = JSON.parse(response.body)['id']

      logger.debug("Receptor response: registering message #{msg_id}".tap { |msg| msg << " req: #{body[:payload]}" if ENV["LOG_ALL_RECEPTOR_MESSAGES"]&.to_i != 0 })
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

    def response_success(msg_id, message_type, serial, response)
      response_lock.synchronize do
        if message_type == MESSAGE_TYPE_RESPONSE
          pages[serial] = {:body => JSON.parse(response["body"]), :status => response["status"]}
        elsif message_type == MESSAGE_TYPE_EOF
          join_pages
          response_waiting.signal
        else
          self.response_exception = ReceptorController::Client::UnknownResponseTypeError.new("#{log_message_common}[MSG: #{msg_id}]")
          response_waiting.signal
        end
      end
    end

    def response_error(msg_id, response_code, serial, err_message)
      response_lock.synchronize do
        self.pages = {}
        self.response_data = nil
        self.response_exception = ReceptorController::Client::ResponseError.new("#{err_message} serial: #{serial}, code: #{response_code}, #{log_message_common} [MSG: #{msg_id}]")
        response_waiting.signal
      end
    end

    def response_timeout(msg_id)
      response_lock.synchronize do
        self.pages = {}
        self.response_data = nil
        self.response_exception = ReceptorController::Client::ResponseTimeoutError.new("Timeout (#{log_message_common}) [MSG: #{msg_id}]")
        response_waiting.signal
      end
    end

    def join_pages
      data = pages.keys.sort.each_with_object({}) do |num, acc|
        # If we have pages of data
        next(pages[num]) unless pages[num][:body].key?("results")

        acc["count"].nil? ? acc["count"] = pages[num][:body]["count"] : acc["count"] += pages[num][:body]["count"]
        acc["results"].nil? ? acc["results"] = pages[num][:body]["results"] : acc["results"] << (pages[num][:body]["results"])
      end

      status = pages.values.all? { |page| page[:status] == 200 }

      self.response_data = {"body" => data.to_json, "status" => status ? 200 : 0}
    end

    private

    attr_accessor :response_data, :response_exception, :response_lock, :response_waiting, :pages

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
