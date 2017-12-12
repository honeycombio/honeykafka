honeykaf is a kafka consumer that expects JSON messages on a kafka topic and sends them on to Honeycomb

It expects to read from a kafka topic populated by events with two keys, `meta` and `data`. Meta contains information about how honeykaf should treat the event and `data` contains the event itself. All keys in "meta" are optional; flags to `honeykaf` will provide defaults should fields be missing from the `meta` object.

Here is an example of the schema:
```
{
	"meta": {
		"presamplerate": 1,
		"goal_samplerate": 1,
		"dynsample_keys": ["key1","key2"],
		"timestamp":"2017-12-04T01:02:03.456Z",
		"dataset":"myds",
		"writekey":"abcabc123123"
	},
	"data":{
		"key1":"val1",
		"key2":"val2",
		"key3":"val3"
	}
}
```

Defaults:

Unless provided as flags to `honeykaf`, the following defaults apply:
* `presamplerate`: 1
* `goal_samplerate`: 1
* `dynsample_keys`: empty list
* `timestamp`: current time
* `dataset`: "honeykaf"
* `writekey`: empty string

Definitions:

* `presamplerate`: If you are sampling events before submitting them to kafka, this is the rate at which you are sampling. For example, if for every 10 events your application processes, you only submit one to kafka, `presamplerate` should be `10`.
* `goal_samplerate`: If you wish `honeykaf` to apply a dynamic sampling algorithm to the events it consumes, it will use this field as the goal sample rate and the `dynsample_keys` list as the key for the dynamic sample. Sampling done by `honeykaf` is in addition to any sampling already done (and identified by the `presamplerate` key). `goal_samplerate` and `dynsample_keys` must be specified together.
* `dynsample_keys`: This is the list of fields to use to form the key for the dynamic sampling algorithm. `goal_samplerate` and `dynsample_keys` must be specified together.
* `timestamp`: the time for this event. If absent, current time will be used.
* `dataset`: the Honeycomb dataset to which to send this event
* `writekey`: the Honeycomb write key to use when sending this event

Internally, each unique combination of `goal_samplerate`, `dynsample_keys`, `dataset` and `writekey` will have its own dynsampler instance for maintaining state.  In order for `honeykaf` to efficiently calcualte its dynamic sampling, try to limit the number of unique combinations of those arguments. As the number of dynsampler instances grows, `honeykaf`'s memory utilization will also grow. Watching that metric will let you gauge whether your flow is sufficiently constrained.


