// Copyright 2016-2017 The grok_exporter Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exporter

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fstab/grok_exporter/config/v2"
	"github.com/fstab/grok_exporter/templates"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

type Match struct {
	Labels map[string]string
	Value  float64
}

type Metric interface {
	Name() string
	Collector() prometheus.Collector

	// Returns the match if the line matched, and nil if the line didn't match.
	ProcessMatch(line string) (*Match, error)
	// Returns the match if the delete pattern matched, nil otherwise.
	ProcessDeleteMatch(line string) (*Match, error)
	// Remove old metrics
	ProcessRetention() error
	// Returns push flag
	NeedPush() bool
	// Return job_name when pushing matric
	JobName() string
	// Return pushgateway addr
	PushgatewayAddr() string
}

// Common values for incMetric and observeMetric
type metric struct {
	name            string
	regex           *OnigurumaRegexp
	deleteRegex     *OnigurumaRegexp
	retention       time.Duration
	push            bool
	jobName         string
	pushgatewayAddr string
}

type observeMetric struct {
	metric
	valueTemplate templates.Template
}

type metricWithLabels struct {
	metric
	labelTemplates       []templates.Template
	deleteLabelTemplates []templates.Template
	// add grouping key template
	groupingKeyTemplates []templates.Template
	labelValueTracker    LabelValueTracker
}

type observeMetricWithLabels struct {
	metricWithLabels
	valueTemplate templates.Template
}

type counterMetric struct {
	metric
	counter prometheus.Counter
}

type counterVecMetric struct {
	metricWithLabels
	counterVec *prometheus.CounterVec
}

type gaugeMetric struct {
	observeMetric
	cumulative bool
	gauge      prometheus.Gauge
}

type gaugeVecMetric struct {
	observeMetricWithLabels
	cumulative bool
	gaugeVec   *prometheus.GaugeVec
}

type histogramMetric struct {
	observeMetric
	histogram prometheus.Histogram
}

type histogramVecMetric struct {
	observeMetricWithLabels
	histogramVec *prometheus.HistogramVec
}

type summaryMetric struct {
	observeMetric
	summary prometheus.Summary
}

type summaryVecMetric struct {
	observeMetricWithLabels
	summaryVec *prometheus.SummaryVec
}

type deleterMetric interface {
	Delete(prometheus.Labels) bool
}

func (m *metric) Name() string {
	return m.name
}

// return push flag
func (m *metric) NeedPush() bool {
	if m.push {
		return m.push
	}
	return true
}

// return job name when pushing metric
func (m *metric) JobName() string {
	if len(m.jobName) > 0 {
		return m.jobName
	} else {
		return "grok_exporter"
	}
}

func (m *counterMetric) Collector() prometheus.Collector {
	return m.counter
}

func (m *counterVecMetric) Collector() prometheus.Collector {
	return m.counterVec
}

func (m *gaugeMetric) Collector() prometheus.Collector {
	return m.gauge
}

func (m *gaugeVecMetric) Collector() prometheus.Collector {
	return m.gaugeVec
}

func (m *histogramMetric) Collector() prometheus.Collector {
	return m.histogram
}

func (m *histogramVecMetric) Collector() prometheus.Collector {
	return m.histogramVec
}

func (m *summaryMetric) Collector() prometheus.Collector {
	return m.summary
}

func (m *summaryVecMetric) Collector() prometheus.Collector {
	return m.summaryVec
}

func (m *metric) processMatch(line string, cb func()) (*Match, error) {
	matchResult, err := m.regex.Match(line)
	if err != nil {
		return nil, fmt.Errorf("error processing metric %v: %v", m.Name(), err.Error())
	}
	defer matchResult.Free()
	if matchResult.IsMatch() {
		cb()
		return &Match{
			Value: 1.0,
		}, nil
	} else {
		return nil, nil
	}
}

func (m *observeMetric) processMatch(line string, cb func(value float64)) (*Match, error) {
	matchResult, err := m.regex.Match(line)
	if err != nil {
		return nil, fmt.Errorf("error processing metric %v: %v", m.Name(), err.Error())
	}
	defer matchResult.Free()
	if matchResult.IsMatch() {
		floatVal, err := floatValue(m.Name(), matchResult, m.valueTemplate)
		if err != nil {
			return nil, err
		}
		cb(floatVal)
		return &Match{
			Value: floatVal,
		}, nil
	} else {
		return nil, nil
	}
}

func (m *metricWithLabels) processMatch(line string, vec deleterMetric, cb func(labels map[string]string)) (*Match, error) {
	matchResult, err := m.regex.Match(line)
	if err != nil {
		return nil, fmt.Errorf("error while processing metric %v: %v", m.Name(), err.Error())
	}
	defer matchResult.Free()
	if matchResult.IsMatch() {
		labels, err := labelValues(m.Name(), matchResult, m.labelTemplates)

		if err != nil {
			return nil, err
		}
		m.labelValueTracker.Observe(labels)
		cb(labels)

		// push metric
		if m.NeedPush() {
			groupingKey, err := labelValues(m.Name(), matchResult, m.groupingKeyTemplates)
			pushMetric(*m, vec, groupingKey, labels)
		}
		return &Match{
			Value:  1.0,
			Labels: labels,
		}, nil
	} else {
		return nil, nil
	}
}

func (m *observeMetricWithLabels) processMatch(line string, vec deleterMetric, cb func(value float64, labels map[string]string)) (*Match, error) {
	matchResult, err := m.regex.Match(line)
	if err != nil {
		return nil, fmt.Errorf("error processing metric %v: %v", m.Name(), err.Error())
	}
	defer matchResult.Free()
	if matchResult.IsMatch() {
		floatVal, err := floatValue(m.Name(), matchResult, m.valueTemplate)
		if err != nil {
			return nil, err
		}
		labels, err := labelValues(m.Name(), matchResult, m.labelTemplates)
		if err != nil {
			return nil, err
		}

		m.labelValueTracker.Observe(labels)
		cb(floatVal, labels)

		// push metric
		if m.NeedPush() {
			groupingKey, err := labelValues(m.Name(), matchResult, m.groupingKeyTemplates)
			pushMetric(m.metricWithLabels, vec, groupingKey, labels)
		}
		return &Match{
			Value:  floatVal,
			Labels: labels,
		}, nil
	} else {
		return nil, nil
	}
}

func (m *metric) ProcessDeleteMatch(line string) (*Match, error) {
	if m.deleteRegex == nil {
		return nil, nil
	}
	return nil, fmt.Errorf("error processing metric %v: delete_match is currently only supported for metrics with labels.", m.Name())
}

func (m *metric) ProcessRetention() error {
	if m.retention == 0 {
		return nil
	}
	return fmt.Errorf("error processing metric %v: retention is currently only supported for metrics with labels.", m.Name())
}

func (m *metricWithLabels) processDeleteMatch(line string, vec deleterMetric) (*Match, error) {
	if m.deleteRegex == nil {
		return nil, nil
	}
	matchResult, err := m.deleteRegex.Match(line)
	if err != nil {
		return nil, fmt.Errorf("error processing metric %v: %v", m.name, err.Error())
	}
	defer matchResult.Free()
	if matchResult.IsMatch() {
		deleteLabels, err := labelValues(m.Name(), matchResult, m.deleteLabelTemplates)
		if err != nil {
			return nil, err
		}
		matchingLabels, err := m.labelValueTracker.DeleteByLabels(deleteLabels)
		if err != nil {
			return nil, err
		}
		// delete metric from pushgateway first
		if m.NeedPush() {
			groupingKey, err := labelValues(m.Name(), matchResult, m.groupingKeyTemplates)
			deleteMetric(m, groupingKey)
		}
		for _, matchingLabel := range matchingLabels {
			vec.Delete(matchingLabel)
		}
		return &Match{
			Labels: deleteLabels,
		}, nil
	} else {
		return nil, nil
	}
}

func (m *metricWithLabels) processRetention(vec deleterMetric) error {
	if m.retention != 0 {
		for _, label := range m.labelValueTracker.DeleteByRetention(m.retention) {
			vec.Delete(label)
		}
	}
	return nil
}

func (m *counterMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func() {
		m.counter.Inc()
	})
}

func (m *counterVecMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, m.counterVec, func(labels map[string]string) {
		m.counterVec.With(labels).Inc()
	})
}

func (m *counterVecMetric) ProcessDeleteMatch(line string) (*Match, error) {
	return m.processDeleteMatch(line, m.counterVec)
}

func (m *counterVecMetric) ProcessRetention() error {
	return m.processRetention(m.counterVec)
}

func (m *gaugeMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64) {
		if m.cumulative {
			m.gauge.Add(value)
		} else {
			m.gauge.Set(value)
		}
	})
}

func (m *gaugeVecMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, m.gaugeVec, func(value float64, labels map[string]string) {
		if m.cumulative {
			m.gaugeVec.With(labels).Add(value)
		} else {
			m.gaugeVec.With(labels).Set(value)
		}
	})
}

func (m *gaugeVecMetric) ProcessDeleteMatch(line string) (*Match, error) {
	return m.processDeleteMatch(line, m.gaugeVec)
}

func (m *gaugeVecMetric) ProcessRetention() error {
	return m.processRetention(m.gaugeVec)
}

func (m *histogramMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64) {
		m.histogram.Observe(value)
	})
}

func (m *histogramVecMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, m.histogramVec, func(value float64, labels map[string]string) {
		m.histogramVec.With(labels).Observe(value)
	})
}

func (m *histogramVecMetric) ProcessDeleteMatch(line string) (*Match, error) {
	return m.processDeleteMatch(line, m.histogramVec)
}

func (m *histogramVecMetric) ProcessRetention() error {
	return m.processRetention(m.histogramVec)
}

func (m *summaryMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64) {
		m.summary.Observe(value)
	})
}

func (m *summaryVecMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, m.summaryVec, func(value float64, labels map[string]string) {
		m.summaryVec.With(labels).Observe(value)
	})
}

func (m *summaryVecMetric) ProcessDeleteMatch(line string) (*Match, error) {
	return m.processDeleteMatch(line, m.summaryVec)
}

func (m *summaryVecMetric) ProcessRetention() error {
	return m.processRetention(m.summaryVec)
}

func pushMetric(m metricWithLabels, vec deleterMetric, groupingKey map[string]string, labels map[string]string) error {
	collector, ok := vec.(prometheus.Collector)
    if !ok {
        return fmt.Errorf("Can not convert deleterMetric to collector")
    }
    r := prometheus.NewRegistry()
	if err := r.Register(collector); err != nil {
		return err
	}
	err := doRequest(m.metric.JobName(), groupingKey, m.pushgatewayAddr, r, "POST")
	if err != nil {
		return err
	}
	//remove metric from local collector
	matchingLabels, err := m.labelValueTracker.DeleteByLabels(labels)
	if err != nil {
		return err
	}
	for _, matchingLabel := range matchingLabels {
		vec.Delete(matchingLabel)
	}
	return nil
}

func deleteMetric(m *metricWithLabels, groupingKey map[string]string) error {
	return doRequest(m.JobName(), groupingKey, m.pushgatewayAddr, nil, "DELETE")
}

func doRequest(job string, groupingKey map[string]string, targetUrl string, g prometheus.Gatherer, method string) error {
	if !strings.Contains(targetUrl, "://") {
		targetUrl = "http://" + targetUrl
	}
	if strings.HasSuffix(targetUrl, "/") {
		targetUrl = targetUrl[:len(targetUrl)-1]
	}

	if strings.Contains(job, "/") {
		return fmt.Errorf("job contains '/' : %s", job)
	}
	urlComponents := []string{url.QueryEscape(job)}
	for ln, lv := range groupingKey {
		if !model.LabelName(ln).IsValid() {
			return fmt.Errorf("groupingKey label has invalid name: %s", ln)
		}
		if strings.Contains(lv, "/") {
			return fmt.Errorf("value of groupingKey label %s contains '/': %s", ln, lv)
		}
		urlComponents = append(urlComponents, ln, lv)
	}

	targetUrl = fmt.Sprintf("%s/metrics/job/%s", targetUrl, strings.Join(urlComponents, "/"))

	buf := &bytes.Buffer{}
	enc := expfmt.NewEncoder(buf, expfmt.FmtProtoDelim)
	if g != nil {
		mfs, err := g.Gather()
		if err != nil {
			return err
		}
		for _, mf := range mfs {
			//ignore checking for pre-existing labels
			enc.Encode(mf)
		}
	}

	var request *http.Request
	var err error
	if method == "DELETE" {
		request, err = http.NewRequest(method, targetUrl, nil)
	} else {
		request, err = http.NewRequest(method, targetUrl, buf)
	}

	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", string(expfmt.FmtProtoDelim))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 202 {
		return fmt.Errorf("unexpected status code %d, method %s", response.StatusCode, method)
	}
	return nil
}

func newMetric(cfg *v2.Config, regex, deleteRegex *OnigurumaRegexp) metric {
	return metric{
		name:            cfg.Metrics.Name,
		regex:           regex,
		deleteRegex:     deleteRegex,
		retention:       cfg.Metrics.Retention,
		push:            cfg.Metrics.Push,
		jobName:         cfg.Metrics.JobName,
		pushgatewayAddr: cfg.Global.PushgatewayAddr,
	}
}

func newMetricWithLabels(cfg *v2.Config, regex, deleteRegex *OnigurumaRegexp) metricWithLabels {
	return metricWithLabels{
		metric:               newMetric(cfg, regex, deleteRegex),
		labelTemplates:       cfg.Metrics.LabelTemplates,
		deleteLabelTemplates: cfg.Metrics.DeleteLabelTemplates,
		groupingKeyTemplates: cfg.Metrics.GroupTemplates,
		labelValueTracker:    NewLabelValueTracker(prometheusLabels(cfg.Metrics.LabelTemplates)),
	}
}

func newObserveMetric(cfg *v2.Config, regex, deleteRegex *OnigurumaRegexp) observeMetric {
	return observeMetric{
		metric:        newMetric(cfg, regex, deleteRegex),
		valueTemplate: cfg.Metrics.ValueTemplate,
	}
}

func newObserveMetricWithLabels(cfg *v2.Config, regex, deleteRegex *OnigurumaRegexp) observeMetricWithLabels {
	return observeMetricWithLabels{
		metricWithLabels: newMetricWithLabels(cfg, regex, deleteRegex),
		valueTemplate:    cfg.Metrics.ValueTemplate,
	}
}

func NewCounterMetric(cfg *v2.Config, regex *OnigurumaRegexp, deleteRegex *OnigurumaRegexp) Metric {
	counterOpts := prometheus.CounterOpts{
		Name: cfg.Metrics.Name,
		Help: cfg.Metrics.Help,
	}
	if len(cfg.Metrics.Labels) == 0 {
		return &counterMetric{
			metric:  newMetric(cfg, regex, deleteRegex),
			counter: prometheus.NewCounter(counterOpts),
		}
	} else {
		return &counterVecMetric{
			metricWithLabels: newMetricWithLabels(cfg, regex, deleteRegex),
			counterVec:       prometheus.NewCounterVec(counterOpts, prometheusLabels(cfg.Metrics.LabelTemplates)),
		}
	}
}

func NewGaugeMetric(cfg *v2.Config, regex *OnigurumaRegexp, deleteRegex *OnigurumaRegexp) Metric {
	gaugeOpts := prometheus.GaugeOpts{
		Name: cfg.Metrics.Name,
		Help: cfg.Metrics.Help,
	}
	if len(cfg.Metrics.Labels) == 0 {
		return &gaugeMetric{
			observeMetric: newObserveMetric(cfg, regex, deleteRegex),
			cumulative:    cfg.Metrics.Cumulative,
			gauge:         prometheus.NewGauge(gaugeOpts),
		}
	} else {
		return &gaugeVecMetric{
			observeMetricWithLabels: newObserveMetricWithLabels(cfg, regex, deleteRegex),
			cumulative:              cfg.Metrics.Cumulative,
			gaugeVec:                prometheus.NewGaugeVec(gaugeOpts, prometheusLabels(cfg.Metrics.LabelTemplates)),
		}
	}
}

func NewHistogramMetric(cfg *v2.Config, regex *OnigurumaRegexp, deleteRegex *OnigurumaRegexp) Metric {
	histogramOpts := prometheus.HistogramOpts{
		Name: cfg.Metrics.Name,
		Help: cfg.Metrics.Help,
	}
	if len(cfg.Metrics.Buckets) > 0 {
		histogramOpts.Buckets = cfg.Buckets
	}
	if len(cfg.Metrics.Labels) == 0 {
		return &histogramMetric{
			observeMetric: newObserveMetric(cfg, regex, deleteRegex),
			histogram:     prometheus.NewHistogram(histogramOpts),
		}
	} else {
		return &histogramVecMetric{
			observeMetricWithLabels: newObserveMetricWithLabels(cfg, regex, deleteRegex),
			histogramVec:            prometheus.NewHistogramVec(histogramOpts, prometheusLabels(cfg.Metrics.LabelTemplates)),
		}
	}
}

func NewSummaryMetric(cfg *v2.Config, regex *OnigurumaRegexp, deleteRegex *OnigurumaRegexp) Metric {
	summaryOpts := prometheus.SummaryOpts{
		Name: cfg.Metrics.Name,
		Help: cfg.Metrics.Help,
	}
	if len(cfg.Metrics.Quantiles) > 0 {
		summaryOpts.Objectives = cfg.Quantiles
	}
	if len(cfg.Metrics.Labels) == 0 {
		return &summaryMetric{
			observeMetric: newObserveMetric(cfg, regex, deleteRegex),
			summary:       prometheus.NewSummary(summaryOpts),
		}
	} else {
		return &summaryVecMetric{
			observeMetricWithLabels: newObserveMetricWithLabels(cfg, regex, deleteRegex),
			summaryVec:              prometheus.NewSummaryVec(summaryOpts, prometheusLabels(cfg.Metrics.LabelTemplates)),
		}
	}
}

func labelValues(metricName string, matchResult *OnigurumaMatchResult, templates []templates.Template) (map[string]string, error) {
	result := make(map[string]string, len(templates))
	for _, t := range templates {
		value, err := evalTemplate(matchResult, t)
		if err != nil {
			return nil, fmt.Errorf("error processing metric %v: %v", metricName, err.Error())
		}
		result[t.Name()] = value
	}
	return result, nil
}

func floatValue(metricName string, matchResult *OnigurumaMatchResult, valueTemplate templates.Template) (float64, error) {
	stringVal, err := evalTemplate(matchResult, valueTemplate)
	if err != nil {
		return 0, fmt.Errorf("error processing metric %v: %v", metricName, err.Error())
	}
	floatVal, err := strconv.ParseFloat(stringVal, 64)
	if err != nil {
		return 0, fmt.Errorf("error processing metric %v: value matches '%v', which is not a valid number.", metricName, stringVal)
	}
	return floatVal, nil
}

func evalTemplate(matchResult *OnigurumaMatchResult, t templates.Template) (string, error) {
	grokValues := make(map[string]string, len(t.ReferencedGrokFields()))
	for _, field := range t.ReferencedGrokFields() {
		value, err := matchResult.Get(field)
		if err != nil {
			return "", err
		}
		grokValues[field] = value
	}
	return t.Execute(grokValues)
}

func prometheusLabels(templates []templates.Template) []string {
	promLabels := make([]string, 0, len(templates))
	for _, t := range templates {
		promLabels = append(promLabels, t.Name())
	}
	return promLabels
}
