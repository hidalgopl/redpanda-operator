// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package multicluster

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	redpandav1alpha2 "github.com/redpanda-data/redpanda-operator/operator/api/redpanda/v1alpha2"
)

// secrets returns all Secrets for the given RenderState.
func secrets(state *RenderState) ([]*corev1.Secret, error) {
	var secrets []*corev1.Secret
	secrets = append(secrets, secretSTSLifecycle(state))
	saslUsers, err := secretSASLUsers(state)
	if err != nil {
		return nil, err
	}
	if saslUsers != nil {
		secrets = append(secrets, saslUsers)
	}
	for _, pool := range state.pools {
		secrets = append(secrets, secretConfigurator(state, pool))
		if fsValidator := secretFSValidator(state, pool); fsValidator != nil {
			secrets = append(secrets, fsValidator)
		}
	}
	if bootstrapUser := secretBootstrapUser(state); bootstrapUser != nil {
		secrets = append(secrets, bootstrapUser)
	}
	return secrets, nil
}

// secretSTSLifecycle returns the lifecycle scripts Secret for the StatefulSet.
func secretSTSLifecycle(state *RenderState) *corev1.Secret {
	p := scriptParamsFromState(state)

	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-sts-lifecycle", state.fullname()),
			Namespace: state.namespace,
			Labels:    state.commonLabels(),
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"common.sh":    lifecycleCommonSh(p),
			"postStart.sh": lifecyclePostStartSh(p),
			"preStop.sh":   lifecyclePreStopSh(p),
		},
	}
}

// secretSASLUsers returns the SASL users secret, if applicable.
func secretSASLUsers(state *RenderState) (*corev1.Secret, error) {
	if !state.Spec().Auth.IsSASLEnabled() {
		return nil, nil
	}

	sasl := state.Spec().Auth.SASL
	secretRef := ptr.Deref(sasl.SecretRef, "")

	if secretRef == "" {
		return nil, fmt.Errorf("auth.sasl.secretRef cannot be empty when auth.sasl.enabled=true")
	}
	if len(sasl.Users) == 0 {
		return nil, nil
	}

	defaultMechanism := sasl.GetMechanism()

	var usersTxt []string
	for _, user := range sasl.Users {
		mechanism := ptr.Deref(user.Mechanism, defaultMechanism)
		usersTxt = append(usersTxt, fmt.Sprintf("%s:%s:%s",
			ptr.Deref(user.Name, ""),
			ptr.Deref(user.Password, ""),
			mechanism,
		))
	}

	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretRef,
			Namespace: state.namespace,
			Labels:    state.commonLabels(),
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"users.txt": strings.Join(usersTxt, "\n"),
		},
	}, nil
}

// secretBootstrapUser returns the bootstrap user Secret. The password is
// determined centrally by syncBootstrapUser and injected via RenderState so
// that every cluster gets the same credential. The secret is marked Immutable
// so that Kubernetes rejects any future mutations — password rotation requires
// deleting and re-creating the secret.
func secretBootstrapUser(state *RenderState) *corev1.Secret {
	if !state.Spec().Auth.IsSASLEnabled() || state.bootstrapPassword == "" {
		return nil
	}

	sasl := state.Spec().Auth.SASL
	if sasl.BootstrapUser != nil && sasl.BootstrapUser.SecretKeyRef != nil {
		return nil
	}

	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-bootstrap-user", state.fullname()),
			Namespace: state.namespace,
			Labels:    state.commonLabels(),
		},
		Immutable: ptr.To(true),
		Type:      corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"password": state.bootstrapPassword,
		},
	}
}

// secretFSValidator returns the filesystem validator Secret for a pool.
func secretFSValidator(state *RenderState, pool *redpandav1alpha2.NodePool) *corev1.Secret {
	if pool.Spec.InitContainers == nil || !pool.Spec.InitContainers.FSValidator.IsEnabled() {
		return nil
	}

	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%.49s-fs-validator", state.poolFullname(pool)),
			Namespace: state.namespace,
			Labels:    state.commonLabels(),
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"fsValidator.sh": fsValidatorSh,
		},
	}
}

// secretConfigurator returns the configurator script Secret for a pool.
func secretConfigurator(state *RenderState, pool *redpandav1alpha2.NodePool) *corev1.Secret {
	p := scriptParamsFromState(state)

	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%.51s-configurator", state.poolFullname(pool)),
			Namespace: state.namespace,
			Labels:    state.commonLabels(),
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"configurator.sh": configuratorSh(p),
		},
	}
}
