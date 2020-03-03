require "receptor_controller/client/directive_non_blocking"

RSpec.describe ReceptorController::Client::DirectiveNonBlocking do
  let(:external_tenant) { '0000001' }
  let(:organization_id) { '000001' }
  let(:identity) do
    {"x-rh-identity" => Base64.strict_encode64({"identity" => {"account_number" => external_tenant, "user" => {"is_org_admin" => true}, "internal" => {"org_id" => organization_id}}}.to_json)}
  end
  let(:headers) do
    {"Content-Type"    => "application/json",
     "User-Agent"      => "Faraday v1.0.0",
     "Accept"          => "*/*",
     "Accept-Encoding" => 'gzip;q=1.0,deflate;q=0.6,identity;q=0.3'}.merge(identity)
  end
  let(:receptor_scheme) { 'http' }
  let(:receptor_host) { 'localhost:9090' }
  let(:receptor_node) { 'testing-receptor' }
  let(:receptor_config) do
    ReceptorController::Client::Configuration.new do |config|
      config.controller_scheme = receptor_scheme
      config.controller_host   = receptor_host
    end
  end
  let(:receptor_client) do
    client = ReceptorController::Client.new
    client.identity_header = identity
    client
  end


  let(:satellite_uid) { '1234567890' }
  let(:payload) { {'satellite_instance_id' => satellite_uid.to_s}.to_json }
  let(:directive) { 'receptor_satellite:health_check' }

  subject { described_class.new(:name => directive, :account => external_tenant, :node_id => receptor_node, :payload => payload, :client => receptor_client) }

  describe "#call" do
    it "makes POST /job request to receptor, registers received message ID and returns it" do
      response = {"id" => '1234'}

      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/job")
        .with(:body    => subject.default_body.to_json,
              :headers => headers)
        .to_return(:status => 200, :body => response.to_json, :headers => {})

      expect(subject.response_worker).to receive(:register_message).with(response['id'], subject)

      expect(subject.call).to eq(response['id'])
    end

    it "makes POST /job request to receptor, doesn't register message and returns nil in case of error" do
      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/job")
        .with(:body    => subject.default_body.to_json,
              :headers => headers)
        .to_return(:status => 401, :body => {"errors" => [{"status" => 401, "detail" => "Unauthorized"}]}.to_json, :headers => {})

      expect(subject.response_worker).not_to receive(:register_message)

      expect(subject.call).to be_nil
    end

    it "makes a POST request and returns disconnected if receptor unavailable" do
      allow(Faraday).to receive(:post).and_raise(Faraday::ConnectionFailed, "Failed to open TCP connection to #{receptor_host}")

      expect(subject.response_worker).not_to receive(:register_message)

      expect(subject.call).to be_nil
    end
  end

  context "callbacks" do
    it "calls success/eof blocks" do
      response_id = '1234'

      # Callbacks
      result = {}
      subject
        .on_success do |msg_id, payload|
          result[:first] = {:id => msg_id, :payload => payload}
        end
        .on_success do |msg_id, payload|
          result[:second] = {:id => msg_id, :payload => payload}
        end
        .on_eof do |msg_id|
          result[:eof] = {:id => msg_id}
        end

      # HTTP request
      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/job")
        .with(:body    => subject.default_body.to_json,
              :headers => headers)
        .to_return(:status => 200, :body => {'id' => response_id}.to_json, :headers => {})

      subject.call

      # Kafka response - 'response' type
      expect(subject).to receive(:response_success).twice.and_call_original
      message_type, payload = 'response', 'Testing payload'
      response_payload = {'code' => 0, 'in_response_to' => response_id, 'message_type' => message_type, 'payload' => payload}
      response = double
      allow(response).to receive(:payload).and_return(response_payload.to_json)
      subject.response_worker.send(:process_message, response)

      # Kafka response - 'eof' type
      message_type = 'eof'
      response_payload = {'code' => 0, 'in_response_to' => response_id, 'message_type' => message_type, 'payload' => nil}
      allow(response).to receive(:payload).and_return(response_payload.to_json)
      subject.response_worker.send(:process_message, response)


      # Result containing both "on_success" blocks
      block_result = {:id => response_id, :payload => payload}
      expect(result).to eq(:first  => block_result,
                           :second => block_result,
                           :eof    => {:id => response_id})
    end
  end
end
