// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package console

import (
	"fmt"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/redpanda-data/console/backend/pkg/config"
	"github.com/redpanda-data/redpanda-operator/gotohelm/helmette"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

func ServiceMonitor(state *RenderState) *monitoringv1.ServiceMonitor {
	if !state.Values.Monitoring.Enabled {
		return nil
	}
	endpoint := monitoringv1.Endpoint{
		Interval:    state.Values.Monitoring.ScrapeInterval,
		Path:        "/admin/metrics",
		Port:        "admin",
		EnableHttp2: ptr.To(state.Values.Monitoring.EnableHTTP2),
		Scheme:      "http",
	}
	consoleConfig := config.Server{}
	err := yaml.Unmarshal(state.Values.Config["server"].([]byte), &consoleConfig)
	if err != nil {
		fmt.Printf("error unmarshalling config: %v\n", err)
		// TODO: decide what to do here
	} else {
		if consoleConfig.TLS.Enabled {
			tlsConfig := &monitoringv1.TLSConfig{
				CertFile: consoleConfig.TLS.CertFilepath,
				KeyFile:  consoleConfig.TLS.KeyFilepath,
			}
			endpoint.TLSConfig = tlsConfig
		}
	}
	return &monitoringv1.ServiceMonitor{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "monitoring.coreos.com/v1",
			Kind:       monitoringv1.ServiceMonitorsKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      state.FullName(),
			Namespace: state.Namespace,
			Labels:    helmette.Merge(state.Labels(nil), state.Values.Monitoring.Labels),
		},
		Spec: monitoringv1.ServiceMonitorSpec{
			Endpoints: []monitoringv1.Endpoint{endpoint},
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"monitoring.redpanda.com/enabled": "true",
					"app.kubernetes.io/name":          state.ReleaseName,
					"app.kubernetes.io/instance":      state.FullName(),
				},
			},
		},
	}
}
