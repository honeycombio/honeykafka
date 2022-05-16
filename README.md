# honeykafka

[![OSS Lifecycle](https://img.shields.io/osslifecycle/honeycombio/honeykafka)](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)

**STATUS: this project is [archived](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md).** You can use the [Kafka receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/kafkareceiver) with OpenTelemetry to send telemetry data from Kafka to Honeycomb.

Questions? You can chat with us in the **Honeycomb Pollinators** Slack. You can find a direct link to request an invite in [Spread the Love: Appreciating Our Pollinators Community](https://www.honeycomb.io/blog/spread-the-love-appreciating-our-pollinators-community/).

---

`honeykafka` is a Kafka consumer that expects JSON messages on a kafka topic and sends them on to [Honeycomb](https://honeycomb.io)

**This repo is in beta!** Please file an issue if this doesn't behave as expected.

## Installation

Install from source:

```
go get github.com/honeycombio/honeykafka
```

to install to a specific path:

```
GOPATH=/usr/local go get github.com/honeycombio/honeykafka
```

the binary will install to `/usr/local/bin/honeykafka`

## Usage

```
honeykafka --writekey=YOUR_WRITE_KEY --dataset='Existing JSON Stream' \
  --kafka.server=10.0.0.1 --kafka.topic=eventstream --kafka.partition=0
```

`honeykafka` expects to consume from a single partition on a kafka topic. You should launch one copy of honeykafka per partition for your topic.

The flags given to honeykafka will be used when the message from kafka does not specify a field - the kafka message overrides the flags given to the binary. This is the case for:
* writekey
* dataset
* sample rate
* dynamic sampler keys

The intention is that the producer of the message (the application that puts the event on to the kafka queue) gets to choose the target dataset, sample rate, and dynamic sampler keys to use for that event. Different teams can then share the same kafka infrastructure while sending data to their own dataset.

If an event is pushed into the kafka queue without one of these fields, the flags given to `honeykafka` will be used as default values. The operator can thereby set a default `writekey` or `sample rate` so that individual publishers can opt out of needing those fields.

## Kafka Message Format

`honeykafka` expects to read from a kafka topic populated by events with two keys, `meta` and `data`. Meta contains information about how `honeykafka` should treat the event and `data` contains the event itself. All keys in `meta` are optional; flags to `honeykafka` will provide defaults should fields be missing from the `meta` object.

Here is an example of the schema:
```
{
	"meta": {
		"writekey":"abcabc123123",
		"dataset":"myds",
		"presamplerate": 1,
		"goal_samplerate": 1,
		"dynsample_keys": ["key1","key2"],
		"timestamp":"2017-12-04T01:02:03.456Z"
	},
	"data":{
		"key1":"val1",
		"key2":"val2",
		"key3":"val3"
	}
}
```

Defaults:

Unless provided as flags to `honeykafka`, the following defaults apply:
* `presamplerate`: 1
* `goal_samplerate`: 1
* `dynsample_keys`: empty list
* `timestamp`: current time
* `dataset`: "honeykafka"
* `writekey`: empty string

Definitions:

* `presamplerate`: If you are sampling events before submitting them to kafka, this is the rate at which you are sampling. For example, if for every 10 events your application processes, you only submit one to kafka, `presamplerate` should be `10`.
* `goal_samplerate`: If you wish `honeykafka` to apply a dynamic sampling algorithm to the events it consumes, it will use this field as the goal sample rate and the `dynsample_keys` list as the key for the dynamic sample. Sampling done by `honeykafka` is in addition to any sampling already done (and identified by the `presamplerate` key). `goal_samplerate` and `dynsample_keys` must be specified together.
* `dynsample_keys`: This is the list of fields to use to form the key for the dynamic sampling algorithm. `goal_samplerate` and `dynsample_keys` must be specified together.
* `timestamp`: the time for this event. If absent, current time will be used.
* `dataset`: the Honeycomb dataset to which to send this event
* `writekey`: the Honeycomb write key to use when sending this event

Internally, each unique combination of `goal_samplerate`, `dynsample_keys`, `dataset` and `writekey` will have its own dynsampler instance for maintaining state.  In order for `honeykafka` to efficiently calcualte its dynamic sampling, try to limit the number of unique combinations of those arguments. As the number of dynsampler instances grows, `honeykafka`'s memory utilization will also grow. Watching that metric will let you gauge whether your flow is sufficiently constrained.


