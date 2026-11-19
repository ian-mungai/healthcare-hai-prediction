// Renders the Grafana notification template offline, for the checks of scripts/infrastructure/sns_template_cases.py.
//
// Usage: go run monitoring/grafana/render_notice.go TEMPLATE_FILE PAYLOAD_FILE [MODE]
//
// TEMPLATE_FILE holds the text of the template group (the "template" value of templates.yml). PAYLOAD_FILE holds the JSON of one notification (status,
// alerts with status, labels, annotations, startsAt and endsAt, commonLabels and commonAnnotations) or a JSON list of notifications. The program prints one JSON
// object with the subject and the message of a notification, or a list of them: the results of the templates hai.sns.subject and hai.sns.message.
//
// This is not Grafana. It runs the Go text/template engine that Grafana uses, with stand-ins for the functions of Grafana's notification templates that the
// template reads (toUpper, tz, date and the others of the documented list) and the data fields and methods of the notification data. MODE is zero (the
// default, which reads a missing label as an empty string, as Alertmanager does) or default (which prints "<no value>" for a label that is missing, so a
// template that reads a label or an annotation without a guard shows it).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"time"
)

// KV is a set of key value pairs, as in Grafana.
type KV map[string]string

// SortedPairs returns the pairs sorted by name.
func (kv KV) SortedPairs() []Pair {
	names := kv.Names()
	pairs := make([]Pair, 0, len(names))
	for _, name := range names {
		pairs = append(pairs, Pair{Name: name, Value: kv[name]})
	}
	return pairs
}

// Names returns the sorted names.
func (kv KV) Names() []string {
	names := make([]string, 0, len(kv))
	for name := range kv {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Pair is one name and value.
type Pair struct {
	Name  string
	Value string
}

// Alert is one alert of a notification.
type Alert struct {
	Status       string    `json:"status"`
	Labels       KV        `json:"labels"`
	Annotations  KV        `json:"annotations"`
	StartsAt     time.Time `json:"startsAt"`
	EndsAt       time.Time `json:"endsAt"`
	GeneratorURL string    `json:"generatorURL"`
	Fingerprint  string    `json:"fingerprint"`
}

// Alerts is the list of alerts.
type Alerts []Alert

// Firing returns the firing alerts.
func (alerts Alerts) Firing() []Alert { return alerts.with("firing") }

// Resolved returns the resolved alerts.
func (alerts Alerts) Resolved() []Alert { return alerts.with("resolved") }

func (alerts Alerts) with(status string) []Alert {
	found := []Alert{}
	for _, alert := range alerts {
		if alert.Status == status {
			found = append(found, alert)
		}
	}
	return found
}

// Data is the notification data.
type Data struct {
	Receiver          string
	Status            string
	Alerts            Alerts
	GroupLabels       KV
	CommonLabels      KV
	CommonAnnotations KV
	ExternalURL       string
}

type payload struct {
	Status            string `json:"status"`
	Alerts            Alerts `json:"alerts"`
	CommonLabels      KV     `json:"commonLabels"`
	CommonAnnotations KV     `json:"commonAnnotations"`
}

func functions() template.FuncMap {
	return template.FuncMap{
		"toUpper":   strings.ToUpper,
		"toLower":   strings.ToLower,
		"title":     title,
		"trimSpace": strings.TrimSpace,
		"join":      func(sep string, parts []string) string { return strings.Join(parts, sep) },
		"match": func(pattern string, text string) (bool, error) {
			return regexp.MatchString(pattern, text)
		},
		"reReplaceAll": func(pattern string, replacement string, text string) string {
			return regexp.MustCompile(pattern).ReplaceAllString(text, replacement)
		},
		"stringSlice": func(parts ...string) []string { return parts },
		"date":        func(layout string, moment time.Time) string { return moment.Format(layout) },
		"tz": func(name string, moment time.Time) (time.Time, error) {
			location, err := time.LoadLocation(name)
			if err != nil {
				return moment, err
			}
			return moment.In(location), nil
		},
	}
}

func title(text string) string {
	words := strings.Split(text, " ")
	for index, word := range words {
		if word != "" {
			words[index] = strings.ToUpper(word[:1]) + word[1:]
		}
	}
	return strings.Join(words, " ")
}

func fail(message string, err error) {
	fmt.Fprintf(os.Stderr, "render_notice: %s: %v\n", message, err)
	os.Exit(2)
}

func render(group *template.Template, sent payload) map[string]string {
	data := Data{Receiver: "hai_sns", Status: sent.Status, Alerts: sent.Alerts, CommonLabels: sent.CommonLabels, CommonAnnotations: sent.CommonAnnotations, GroupLabels: KV{}}
	rendered := map[string]string{}
	for key, name := range map[string]string{"subject": "hai.sns.subject", "message": "hai.sns.message"} {
		var out bytes.Buffer
		if err := group.ExecuteTemplate(&out, name, data); err != nil {
			fail("cannot run "+name, err)
		}
		rendered[key] = out.String()
	}
	return rendered
}

func main() {
	if len(os.Args) < 3 {
		fail("usage", fmt.Errorf("go run render_notice.go TEMPLATE_FILE PAYLOAD_FILE [MODE]"))
	}
	text, err := os.ReadFile(os.Args[1])
	if err != nil {
		fail("cannot read the template", err)
	}
	body, err := os.ReadFile(os.Args[2])
	if err != nil {
		fail("cannot read the payload", err)
	}
	options := []string{"missingkey=zero"}
	if len(os.Args) > 3 && os.Args[3] == "default" {
		options = []string{"missingkey=default"}
	}
	group, err := template.New("group").Funcs(functions()).Option(options...).Parse(string(text))
	if err != nil {
		fail("the template does not parse", err)
	}
	var result any
	if strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
		var list []payload
		if err := json.Unmarshal(body, &list); err != nil {
			fail("cannot read the payload list", err)
		}
		results := []map[string]string{}
		for _, sent := range list {
			results = append(results, render(group, sent))
		}
		result = results
	} else {
		var sent payload
		if err := json.Unmarshal(body, &sent); err != nil {
			fail("cannot read the payload", err)
		}
		result = render(group, sent)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fail("cannot write the result", err)
	}
}
