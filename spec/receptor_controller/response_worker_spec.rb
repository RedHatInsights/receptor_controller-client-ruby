require "receptor_controller/client/response_worker"

RSpec.describe ReceptorController::Client::ResponseWorker do
  let(:receiver) { double('receiver') }
  let(:logger) { double('logger') }
  let(:config) { double('config') }

  subject { described_class.new(config, logger) }

  before do
    allow(logger).to receive_messages(%i[debug info warn error fatal])
    allow(config).to receive_messages(:response_timeout => 0, :response_timeout_poll_time => 0)
  end

  describe "#register_message" do
    it "saves callback with message id" do
      msg_id = '1'

      subject.register_message(msg_id, receiver)

      expect(subject.send(:registered_messages)[msg_id]).to match(hash_including(:receiver => receiver))
    end
  end

  describe "#process_message" do
    let(:message) { double('message') }
    let(:message_id) { '1234' }
    let(:response_body) { 'Test response' }
    let(:payload) { {} }
    let(:receiver) { double('receiver') }

    before do
      allow(message).to receive(:payload).and_return(payload.to_json)
      allow(receiver).to receive_messages(:success => nil, :timeout => nil, :error => nil)

      subject.register_message(message_id, receiver, :response_callback => :success, :timeout_callback => :timeout, :error_callback => :error)
    end

    context "receives successful response" do
      let(:payload) { {'code' => 0, 'in_response_to' => message_id, 'message_type' => 'response', 'payload' => response_body} }

      it "and calls response_callback " do
        expect(receiver).to receive(:success).with(message_id, payload['message_type'], payload['payload'])

        subject.send(:process_message, message)
      end

    end

    context "receives error response" do
      let(:payload) { {'code' => 1, 'in_response_to' => message_id, 'message_type' => 'response', 'payload' => response_body} }

      it "and calls response_error" do
        expect(receiver).to receive(:error).with(message_id, payload['code'], payload['payload'])

        subject.send(:process_message, message)
      end
    end

    context "receives invalid message" do
      let(:payload) { 'Wrong message' }
      before do
        allow(message).to receive(:payload).and_return(payload)
      end

      it "logs error" do
        expect(logger).to receive(:error).with(/Receptor response: Failed to parse Kafka response/)

        subject.send(:process_message, message)
      end
    end

    context "receives response without ID" do
      let(:payload) { {'code' => 0, 'message_type' => 'response', 'payload' => response_body} }

      it "logs error" do
        expect(logger).to receive(:error).with(/Receptor response: Message id \(in_response_to\) not received!/)

        subject.send(:process_message, message)
      end
    end

    context "receives unregistered response" do
      let(:payload) { {'code' => 0, 'in_response_to' => '9876', 'message_type' => 'response', 'payload' => response_body} }

      it "does nothing" do
        expect(logger).not_to receive(:error)
        %i[success error timeout].each do |callback|
          expect(receiver).not_to receive(callback)
        end

        subject.send(:process_message, message)
      end
    end
  end

  describe "#check_timeouts" do
    let(:message_id) { '1234' }
    let(:receiver) { double('receiver') }

    before do
      allow(receiver).to receive(:timeout) do
        subject.send(:started).value = false
      end

      subject.register_message(message_id, receiver, :response_callback => :success, :timeout_callback => :timeout, :error_callback => :error)
    end

    it "calls timeout callback if message found" do
      subject.send(:started).value = true

      expect(receiver).to receive(:timeout)

      subject.send(:check_timeouts)
    end
  end
end
