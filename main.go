package main

import (
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/honeycombio/libhoney-go"
	flag "github.com/jessevdk/go-flags"

	"github.com/honeycombio/honeykaf/kafkatail"
	"github.com/honeycombio/honeytail/httime"
	"github.com/honeycombio/honeytail/parsers/htjson"
)

// BuildID is set by Travis CI
var BuildID string

// internal version identifier
var version string

var validParsers = []string{
	"json",
}

// GlobalOptions has all the top level CLI flags that honeytail supports
type GlobalOptions struct {
	APIHost string `hidden:"true" long:"api_host" description:"Host for the Honeycomb API" default:"https://api.honeycomb.io/"`

	ConfigFile string `short:"c" long:"config" description:"Config file for honeytail in INI format." no-ini:"true"`

	SampleRate       uint   `short:"r" long:"samplerate" description:"Only send 1 / N log lines" default:"1"`
	PreSampleKey     string `long:"presample" description:"if the presample key is present, sampling has already been applied at that rate"`
	NumSenders       uint   `short:"P" long:"poolsize" description:"Number of concurrent connections to open to Honeycomb" default:"80"`
	BatchFrequencyMs uint   `long:"send_frequency_ms" description:"How frequently to flush batches" default:"100"`
	BatchSize        uint   `long:"send_batch_size" description:"Maximum number of messages to put in a batch" default:"50"`
	Debug            bool   `long:"debug" description:"Print debugging output"`
	StatusInterval   uint   `long:"status_interval" description:"How frequently, in seconds, to print out summary info" default:"60"`

	Localtime         bool     `long:"localtime" description:"When parsing a timestamp that has no time zone, assume it is in the same timezone as localhost instead of UTC (the default)"`
	Timezone          string   `long:"timezone" description:"When parsing a timestamp use this time zone instead of UTC (the default). Must be specified in TZ format as seen here: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"`
	ScrubFields       []string `long:"scrub_field" description:"For the field listed, apply a one-way hash to the field content. May be specified multiple times"`
	DropFields        []string `long:"drop_field" description:"Do not send the field to Honeycomb. May be specified multiple times"`
	AddFields         []string `long:"add_field" description:"Add the field to every event. Field should be key=val. May be specified multiple times"`
	RequestShape      []string `long:"request_shape" description:"Identify a field that contains an HTTP request of the form 'METHOD /path HTTP/1.x' or just the request path. Break apart that field into subfields that contain components. May be specified multiple times. Defaults to 'request' when using the nginx parser"`
	ShapePrefix       string   `long:"shape_prefix" description:"Prefix to use on fields generated from request_shape to prevent field collision"`
	RequestPattern    []string `long:"request_pattern" description:"A pattern for the request path on which to base the derived request_shape. May be specified multiple times. Patterns are considered in order; first match wins."`
	RequestParseQuery string   `long:"request_parse_query" description:"How to parse the request query parameters. 'whitelist' means only extract listed query keys. 'all' means to extract all query parameters as individual columns" default:"whitelist"`
	RequestQueryKeys  []string `long:"request_query_keys" description:"Request query parameter key names to extract, when request_parse_query is 'whitelist'. May be specified multiple times."`
	BackOff           bool     `long:"backoff" description:"When rate limited by the API, back off and retry sending failed events. Otherwise failed events are dropped. When --backfill is set, it will override this option=true"`
	PrefixRegex       string   `long:"log_prefix" description:"pass a regex to this flag to strip the matching prefix from the line before handing to the parser. Useful when log aggregation prepends a line header. Use named groups to extract fields into the event."`
	DynSample         []string `long:"dynsampling" description:"enable dynamic sampling using the field listed in this option. May be specified multiple times; fields will be concatenated to form the dynsample key. WARNING increases CPU utilization dramatically over normal sampling"`
	DynWindowSec      int      `long:"dynsample_window" description:"measurement window size for the dynsampler, in seconds" default:"30"`
	GoalSampleRate    int      `long:"goal_samplerate" description:"used to hold the desired sample rate and set tailing sample rate to 1"`
	MinSampleRate     int      `long:"dynsample_minimum" description:"if the rate of traffic falls below this, dynsampler won't sample" default:"1"`

	Reqs  RequiredOptions `group:"Required Options"`
	Modes OtherModes      `group:"Other Modes"`

	JSON htjson.Options `group:"JSON Parser Options" namespace:"json"`

	Kafka kafkatail.Options `group:"Kafka Tailing Options" namespace:"kafka"`
}

type RequiredOptions struct {
	WriteKey string `short:"k" long:"writekey" description:"Team write key"`
	Dataset  string `short:"d" long:"dataset" description:"Name of the dataset"`
}

type OtherModes struct {
	Help               bool `short:"h" long:"help" description:"Show this help message"`
	Version            bool `short:"V" long:"version" description:"Show version"`
	WriteDefaultConfig bool `long:"write_default_config" description:"Write a default config file to STDOUT" no-ini:"true"`
	WriteCurrentConfig bool `long:"write_current_config" description:"Write out the current config to STDOUT" no-ini:"true"`

	WriteManPage bool `hidden:"true" long:"write-man-page" description:"Write out a man page"`
}

func main() {
	var options GlobalOptions
	flagParser := flag.NewParser(&options, flag.PrintErrors)
	flagParser.Usage = "-k <writekey> -d <mydata> [optional arguments]\n"

	if extraArgs, err := flagParser.Parse(); err != nil || len(extraArgs) != 0 {
		fmt.Println("Error: failed to parse the command line.")
		if err != nil {
			fmt.Printf("\t%s\n", err)
		} else {
			fmt.Printf("\tUnexpected extra arguments: %s\n", strings.Join(extraArgs, " "))
		}
		usage()
		os.Exit(1)
	}
	// read the config file if present
	if options.ConfigFile != "" {
		ini := flag.NewIniParser(flagParser)
		ini.ParseAsDefaults = true
		if err := ini.ParseFile(options.ConfigFile); err != nil {
			fmt.Printf("Error: failed to parse the config file %s\n", options.ConfigFile)
			fmt.Printf("\t%s\n", err)
			usage()
			os.Exit(1)
		}
	}

	rand.Seed(time.Now().UnixNano())

	if options.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	// set time zone info
	if options.Localtime {
		httime.Location = time.Now().Location()
	}
	if options.Timezone != "" {
		loc, err := time.LoadLocation(options.Timezone)
		if err != nil {
			fmt.Printf("time zone '%s' not successfully parsed.\n", options.Timezone)
			fmt.Printf("see https://en.wikipedia.org/wiki/List_of_tz_database_time_zones for a list of time zones\n")
			fmt.Printf("expected format example: America/Los_Angeles\n")
			fmt.Printf("Specific error: %s\n", err.Error())
			os.Exit(1)
		}
		httime.Location = loc
	}

	setVersionUserAgent(false, "json")
	handleOtherModes(flagParser, options.Modes)
	addParserDefaultOptions(&options)
	sanityCheckOptions(&options)

	// if _, err := libhoney.VerifyWriteKey(libhoney.Config{
	// 	APIHost:  options.APIHost,
	// 	WriteKey: options.Reqs.WriteKey,
	// }); err != nil {
	// 	fmt.Fprintln(os.Stderr, "Could not verify Honeycomb write key: ", err)
	// 	os.Exit(1)
	// }
	run(options)
}

// setVersion sets the internal version ID and updates libhoney's user-agent
func setVersionUserAgent(backfill bool, parserName string) {
	if BuildID == "" {
		version = "dev"
	} else {
		version = BuildID
	}
	if backfill {
		parserName += " backfill"
	}
	libhoney.UserAgentAddition = fmt.Sprintf("kh2/%s (%s)", version, parserName)
}

// handleOtherModes takse care of all flags that say we should just do something
// and exit rather than actually parsing logs
func handleOtherModes(fp *flag.Parser, modes OtherModes) {
	if modes.Version {
		fmt.Println("Honeytail version", version)
		os.Exit(0)
	}
	if modes.Help {
		fp.WriteHelp(os.Stdout)
		fmt.Println("")
		os.Exit(0)
	}
	if modes.WriteManPage {
		fp.WriteManPage(os.Stdout)
		os.Exit(0)
	}
	if modes.WriteDefaultConfig {
		ip := flag.NewIniParser(fp)
		ip.Write(os.Stdout, flag.IniIncludeDefaults|flag.IniCommentDefaults|flag.IniIncludeComments)
		os.Exit(0)
	}
	if modes.WriteCurrentConfig {
		ip := flag.NewIniParser(fp)
		ip.Write(os.Stdout, flag.IniIncludeComments)
		os.Exit(0)
	}
}

func addParserDefaultOptions(options *GlobalOptions) {
	if len(options.DynSample) != 0 {
		// when using dynamic sampling, we make the sampling decision after parsing
		// the content, so we must not tailsample.
		options.GoalSampleRate = int(options.SampleRate)
		options.SampleRate = 1
	}
}

func sanityCheckOptions(options *GlobalOptions) {
	switch {
	case options.Reqs.WriteKey == "" || options.Reqs.WriteKey == "NULL":
		fmt.Println("Write key required to be specified with the --writekey flag.")
		usage()
		os.Exit(1)
	case options.Reqs.Dataset == "":
		fmt.Println("Dataset name required with the --dataset flag.")
		usage()
		os.Exit(1)
	case options.SampleRate == 0:
		fmt.Println("Sample rate must be an integer >= 1")
		usage()
		os.Exit(1)
	case options.RequestParseQuery != "whitelist" && options.RequestParseQuery != "all":
		fmt.Println("request_parse_query flag must be either 'whitelist' or 'all'.")
		usage()
		os.Exit(1)
	case len(options.DynSample) != 0 && options.SampleRate <= 1 && options.GoalSampleRate <= 1:
		fmt.Println("sample rate flag must be set >= 2 when dynamic sampling is enabled")
		usage()
		os.Exit(1)
	}

	// check the prefix regex for validity
	if options.PrefixRegex != "" {
		// make sure the regex is anchored against the start of the string
		if options.PrefixRegex[0] != '^' {
			options.PrefixRegex = "^" + options.PrefixRegex
		}
		// make sure it's valid
		_, err := regexp.Compile(options.PrefixRegex)
		if err != nil {
			fmt.Printf("Prefix regex %s doesn't compile: error %s\n", options.PrefixRegex, err)
			usage()
			os.Exit(1)
		}
	}
}

func usage() {
	fmt.Print(`
Usage: kh2 -k <writekey> -d <mydata> [optional arguments]

For even more detail on required and optional parameters, run
kh2 --help
`)
}
