require 'fluent/filter'
require "dht-sensor-ffi"

module Fluent
  class DHT11Filter < Filter
    # Register this filter as "dht11"
    Fluent::Plugin.register_filter('dht11', self)

    # config_param works like other plugins
    config_param :gpio_no, :integer, :default => 18
    config_param :dht_type, :integer, :default => 11

    def configure(conf)
      super
      # do the usual configuration here
    end

    def start
      super
      # This is the first method to be called when it starts running
      # Use it to allocate resources, etc.
    end

    def shutdown
      super
      # This method is called when Fluentd is shutting down.
      # Use it to free up resources, etc.
    end

    def filter_stream(tag, es)
      # new_es = Fluent::MultiEventStream.new

      # es.each do |time, record|
      #   begin

      #     new_record = record.dup
      #     val = DhtSensor.read(18, 11)
      #     new_record['temp'] = val.temp.round(2).to_s
      #     new_record['humidity'] = val.humidity.round(2).to_s
      #     new_es.add(time, new_record)

      #   rescue => e
      #     router.emit_error_event(tag, time, record, e)
      #   end
      # end
      # new_es

      es.each do |time, record|
        begin
          val = DhtSensor.read(@gpio_no, @dht_type)
          record['temp'] = val.temp.round(2).to_s
          record['humidity'] = val.humidity.round(2).to_s
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      end
      es
    end
  end
end