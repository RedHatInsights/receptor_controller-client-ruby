require "receptor_controller/client"

RSpec.describe ReceptorController::Client do
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
  let(:satellite_uid) { '1234567890' }

  subject { described_class.new(:config => receptor_config) }

  before do
    subject.identity_header = identity
  end

  describe "#connection_status" do
    it "makes a POST request to receptor and returns status key-value if successful" do
      response = {"status" => "connected"}
      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/connection/status")
        .with(:body    => {"account" => external_tenant, "node_id" => receptor_node}.to_json,
              :headers => headers)
        .to_return(:status => 200, :body => response.to_json, :headers => {})

      expect(subject.connection_status(external_tenant, receptor_node)).to eq(response)
    end

    it "makes a POST request to receptor and returns disconnected status in case of error" do
      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/connection/status")
        .with(:body    => {"account" => external_tenant, "node_id" => receptor_node}.to_json,
              :headers => headers)
        .to_return(:status => 401, :body => {"errors" => [{"status" => 401, "detail" => "Unauthorized"}]}.to_json, :headers => {})

      expect(subject.connection_status(external_tenant, receptor_node)).to eq(described_class::STATUS_DISCONNECTED)
    end

    it "makes a POST request and returns disconnected if receptor unavailable" do
      allow(Faraday).to receive(:post).and_raise(Faraday::ConnectionFailed, "Failed to open TCP connection to #{receptor_host}")

      expect(subject.connection_status(external_tenant, receptor_node)).to eq(described_class::STATUS_DISCONNECTED)
    end
  end

  describe "#directive" do
    it "creates blocking or non-blocking directive" do
      %i[blocking non_blocking].each do |type|
        directive = subject.directive(nil,
                                      nil,
                                      :payload   => nil,
                                      :directive => 'xxx',
                                      :type      => type)
        klass     = type == :blocking ? ReceptorController::Client::DirectiveBlocking : ReceptorController::Client::DirectiveNonBlocking
        expect(directive).to be_a_kind_of(klass)
      end
    end
  end
end
